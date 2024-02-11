import pandas as pd
import MySQLdb
import pygrametl
from pygrametl.datasources import CSVSource, SQLSource, PandasSource
from pygrametl.tables import Dimension, FactTable, SlowlyChangingDimension, TypeOneSlowlyChangingDimension, AccumulatingSnapshotFactTable
import duckdb

def fact_orders_scd2(sourceDatabase, dw_conn_wrapper):
    """The function creates the fact table"""
    # Specify the query that will generate the input dataset   
    sql = "select employee_id, concat(employee_first_name,' ', employee_last_name) as employee_name,store_id from employee"
   # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset 
    name_mapping = 'employee_id','employee_name','store_id'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)
    employees_dim = SlowlyChangingDimension(
        name = 'dim_employee_scd2',  # name of the dimensions table in the data warehouse 
        key = 'employee_sk', # name of the primary key
        attributes = ['employee_id', 'valid_from', 'valid_to', 'version', 'employee_name', 'store_id'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
        lookupatts = ['employee_id'], # a sequence with a subset of the attributes that uniquely identify a dimension members. 
        fromatt = 'valid_from', # the name of the attribute telling from when the version becomes valid. Default: None
        toatt = 'valid_to',
        versionatt = 'version') # the name of the attribute telling until when the version is valid. Default: None
    
    for row in source:
        employees_dim.scdensure(row)

    # Specify an optional value to return when a lookup fails
    employees_dim.defaultidvalue = 0

    dw_conn_wrapper.commit()
    
    # Specify the query that will generate the input dataset   
    sql2 = "select order_num, employee_id,customer_id,product_id,sales_channel_id,currency_code,cast(order_quantity as DECIMAL(18, 2)) as order_quantity,cast(total_cost as DECIMAL(18, 2)) as total_cost,cast(total_price as DECIMAL(18, 2)) as total_price,cast(order_date as date) as order_date,cast(ship_date as date) as ship_date,cast(delivery_date as date) as delivery_date,cast(procure_date as date) as procure_date from sales_order"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset 
    name_mapping2 = 'order_num', 'employee_id','customer_id','product_id','sales_channel_id','currency_code','order_quantity','total_cost','total_price','order_date','ship_date','delivery_date','procure_date'  
    source = SQLSource(connection = sourceDatabase, query = sql2, names = name_mapping2)
    fact_table = FactTable(
        name = 'fact_sales_scd2',  # name of the fact table in the data warehouse 
        keyrefs = ['order_num', 'customer_id','product_id','sales_channel_id','employee_sk', 'employee_id'], # a sequence of attribute names that constitute the primary key of the fact tables 
        #(i.e., primary keys in the dimension tables that corresponds to foreign keys in the fact table)
        measures = ['currency_code','order_quantity','total_cost','total_price','order_date','ship_date','delivery_date','procure_date'])  
    # Specify an optional value to return when a lookup fails
    fact_table.defaultidvalue = 0  

    # ensure determines whether the row already exists in the database based on ordernum and inserts
    # if a fact with identical values for ordernum was already present in the fact table. 
    # As we specified compare = False, in case the measure value that already exists in the data warehouse is different from the value in the given row, 
    # the ValueError error is not raised
    for row in source:
        row['employee_sk'] = employees_dim.lookupasof(row, row['order_date'], (True, False), {'employee_id':'employee_id'})
        fact_table.ensure(row, False, {'order_num': 'order_num'})
        
    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()


def main():
    fact_orders_scd2(sourceDatabase, dw_conn_wrapper)

if __name__ == '__main__':
    # Connect to salesdb (OLTP) and salesdwh (OLT)
    sourceDatabase = MySQLdb.connect(database = 'salesdb', user = 'user', password = 'password', port = 42333)
    destDatabase = duckdb.connect(r'C:\Users\katep\OneDrive\Documents\Andreas\dimensional-data-modeling\assets_scripts\salesdwh.duckdb') # Change the path if you have your sales duckDB somewhere else
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection = destDatabase)
    main()