
import pandas as pd
import MySQLdb
import pygrametl
from pygrametl.datasources import CSVSource, SQLSource, PandasSource
from pygrametl.tables import Dimension, FactTable, SlowlyChangingDimension, TypeOneSlowlyChangingDimension, AccumulatingSnapshotFactTable
import duckdb


def dim_stores(sourceDatabase, dw_conn_wrapper):
    """The function creates the stores dimensional table - SCD type 1"""    
    # Specify the query that will generate the input dataset
    sql = "select store_id,latitude,longitude,location,city_name,type,area_code,state_code,state from store s left join city c on s.city_id  = c.city_id left join state st on c.state_id = st.state_id"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset
    name_mapping = 'store_id','latitude','longitude','location','city_name','type','area_code','state_code','state'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)

    # TypeOneSlowlyChangingDimension is created with cityname as a changing attribute i.e. only cityname will be monitored on a change
    destDimension = TypeOneSlowlyChangingDimension(
        name = 'dim_store', # name of the dimensions table in the data warehouse 
        key = 'store_id', # name of the primary key
        attributes =  ['latitude','longitude','location','city_name','type','area_code','state_code','state'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
        type1atts =  ['city_name'], #type1atts: A sequence of attributes that should have type1 updates applied, it cannot intersect with lookupatts. If not given, it is
        #assumed that type1atts = attributes - lookupatts
        lookupatts =  ['store_id'] # a subset of the attributes that uniquely identify a dimension members. These attributes are thus used for looking up members.   
    )

    # scdensure determines whether the row already exists in the database based on storeid
    # and either inserts a new row, or updates the changed attributes (cityname) in the
    # existing row.
    for row in source:
        destDimension.scdensure(row)

    # Specify an optional value to return when a lookup fails
    destDimension.defaultidvalue = 0

    dw_conn_wrapper.commit()


def dim_customers(sourceDatabase, dw_conn_wrapper):
    """The function creates the dimensional table - SCD type 1"""    
    # Specify the query that will generate the input dataset
    sql = "select customer_id,concat(customer_first_name, ' ', customer_last_name) as customer_name from customer "
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset
    name_mapping = 'customer_id','customer_name'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)
    
    # TypeOneSlowlyChangingDimension is created with customername as a changing attribute i.e. only customername will be monitored on a change
    destDimension = TypeOneSlowlyChangingDimension(
        name = 'dim_customer',  # name of the dimensions table in the data warehouse 
        key = 'customer_id', # name of the primary key
        attributes = ['customer_name'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
        lookupatts = ['customer_id']  # a subset of the attributes that uniquely identify a dimension members. These attributes are thus used for looking up members.   
    )

    # scdensure determines whether the row already exists in the database based on customerid
    # and either inserts a new row, or updates the changed attributes (customername) in the
    # existing row.
    for row in source:
        destDimension.scdensure(row)

    # Specify an optional value to return when a lookup fails
    destDimension.defaultidvalue = 0

    dw_conn_wrapper.commit()


def dim_products(sourceDatabase, dw_conn_wrapper):
    """The function creates the dimensional table - SCD type 1"""    
    # Specify the query that will generate the input dataset    
    sql = "select product_id, product_name from product"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset
    name_mapping = 'product_id','product_name'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)

    # TypeOneSlowlyChangingDimension is created with productname as a changing attribute i.e. only productname will be monitored on a change
    destDimension = TypeOneSlowlyChangingDimension(
        name = 'dim_product',  # name of the dimensions table in the data warehouse 
        key = 'product_id', # name of the primary key
        attributes = ['product_name'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
        lookupatts = ['product_id'] # a subset of the attributes that uniquely identify a dimension members. These attributes are thus used for looking up members.  
    )

    # scdensure determines whether the row already exists in the database based on productid
    # and either inserts a new row, or updates the changed attributes (productname) in the
    # existing row.
    for row in source:
        destDimension.scdensure(row)

    # Specify an optional value to return when a lookup fails
    destDimension.defaultidvalue = 0

    dw_conn_wrapper.commit()


def dim_employees_scd1(sourceDatabase, dw_conn_wrapper):
    """The function creates the dimensional table - SCD type 1"""
    # Specify the query that will generate the input dataset   
    sql =  "select employee_id, concat(employee_first_name,' ', employee_last_name) as employee_name,store_id from employee"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset
    name_mapping = 'employee_id','employee_name','store_id'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)

    # TypeOneSlowlyChangingDimension is created with storeid as a changing attribute i.e. only storeid will be monitored on a change   
    destDimension = TypeOneSlowlyChangingDimension(
        name = 'dim_employee_scd1',  # name of the dimensions table in the data warehouse 
        key = 'employee_id', # name of the primary key
        attributes = ['employee_name', 'store_id'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
        type1atts = ['store_id'], #type1atts: A sequence of attributes that should have type1 updates applied, it cannot intersect with lookupatts. If not given, it is
        #assumed that type1atts = attributes - lookupatts
        lookupatts = ['employee_id'] # a subset of the attributes that uniquely identify a dimension members. These attributes are thus used for looking up members.     
    )

    # scdensure determines whether the row already exists in the database based on employeeid
    # and either inserts a new row, or updates the changed attributes (storeid) in the
    # existing row.
    for row in source:
        destDimension.scdensure(row)

    # Specify an optional value to return when a lookup fails   
    destDimension.defaultidvalue = 0  
         
    dw_conn_wrapper.commit()


def dim_channels(sourceDatabase, dw_conn_wrapper):
    """The function creates the dimensional table - SCD type 1"""    
    # Specify the query that will generate the input dataset   
    sql = "select sales_channel_id, sales_channel_name from sales_channel"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset
    name_mapping = 'sales_channel_id','sales_channel_name'
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)

    # TypeOneSlowlyChangingDimension is created with saleschannelname as a changing attribute i.e. only saleschannelname will be monitored on a change 
    destDimension = TypeOneSlowlyChangingDimension(
        name = 'dim_sales_channel',  # name of the dimensions table in the data warehouse 
        key = 'sales_channel_id', # name of the primary key
        attributes = ['sales_channel_name'], #: a sequence of the attribute names in the dimension table. 
        # Should not include the name of the primary key which is given in the key argument.
#        type1atts = ['saleschannelname'],
        lookupatts = ['sales_channel_id'] # a subset of the attributes that uniquely identify a dimension members. These attributes are thus used for looking up members.  
    )

    # scdensure determines whether the row already exists in the database based on saleschannelid
    # and either inserts a new row, or updates the changed attributes (saleschannelname) in the
    # existing row.
    for row in source:
        destDimension.scdensure(row)

    # Specify an optional value to return when a lookup fails
    destDimension.defaultidvalue = 0  
 
    dw_conn_wrapper.commit()


def fact_orders_scd1(sourceDatabase, dw_conn_wrapper):
    """The function creates the fact table based on SCD type 1"""
    # Specify the query that will generate the input dataset   
    sql2 = "select order_num, employee_id,customer_id,product_id,sales_channel_id,currency_code,cast(order_quantity as DECIMAL(18, 2)) as order_quantity,cast(total_cost as DECIMAL(18, 2)) as total_cost,cast(total_price as DECIMAL(18, 2)) as total_price,cast(order_date as date) as order_date,cast(ship_date as date) as ship_date,cast(delivery_date as date) as delivery_date,cast(procure_date as date) as procure_date from sales_order"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset 
    name_mapping2 = 'order_num', 'employee_id','customer_id','product_id','sales_channel_id','currency_code','order_quantity','total_cost','total_price','order_date','ship_date','delivery_date','procure_date'  
    source = SQLSource(connection = sourceDatabase, query = sql2, names = name_mapping2)

    fact_table_1 = FactTable(
        name = 'fact_sales_scd1',  # name of the fact table in the data warehouse 
        keyrefs = ['order_num', 'employee_id','customer_id','product_id','sales_channel_id'], # a sequence of attribute names that constitute the primary key of the fact tables 
        #(i.e., primary keys in the dimension tables that corresponds to foreign keys in the fact table)
        measures = ['currency_code','order_quantity','total_cost','total_price','order_date','ship_date','delivery_date','procure_date'])   # a list of measures
    # Specify an optional value to return when a lookup fails
    fact_table_1.defaultidvalue = 0  

    # ensure determines whether the row already exists in the database based on ordernum and inserts
    # if a fact with identical values for ordernum was already present in the fact table. 
    # As we specified compare = False, in case the measure value that already exists in the data warehouse is different from the value in the given row, 
    # the ValueError error is not raised
    for row in source:
        fact_table_1.ensure(row, False, {'order_num': 'order_num'})

    dw_conn_wrapper.commit()

        
def computelag(row, namemapping, updated):
    """The function calculates the difference in days between different events"""
    if 'ship_date' in updated:
        row['shipment_lag'] = (row['ship_date'] - row['order_date']).days
    if 'delivery_date' in updated:
        row['delivery_lag'] = (row['delivery_date'] - row['ship_date']).days


def fact_orders_acc(sourceDatabase, dw_conn_wrapper):
    asft = AccumulatingSnapshotFactTable(
        name = 'fact_sales_acc',  # name of the fact table in the data warehouse 
        keyrefs = ['order_num', 'employee_id', 'customer_id', 'product_id', 'sales_channel_id'], # a sequence of attribute names that constitute the primary key of the fact tables 
        #(i.e., primary keys in the dimension tables that corresponds to foreign keys in the fact table)
        otherrefs = ['order_date', 'ship_date', 'delivery_date', 'procure_date'], # date columns that should be updated
        measures = ['order_quantity', 'total_cost', 'total_price', 'shipment_lag', 'delivery_lag'], # a list of measures
        factexpander = computelag) # calls the computerlag function to computer the lag measures before the row in the fact table is updated

    # Specify the query that will generate the input dataset  
    sql = "select order_num, employee_id,customer_id,product_id,sales_channel_id,currency_code,cast(order_quantity as DECIMAL(18, 2)) as order_quantity,cast(total_cost as DECIMAL(18, 2)) as total_cost,cast(total_price as DECIMAL(18, 2)) as total_price,cast(order_date as date) as order_date,cast(ship_date as date) as ship_date,cast(delivery_date as date) as delivery_date,cast(procure_date as date) as procure_date from sales_order"
    # Pygrametl will automatically rename columns, in case if names of the input columns are different from the result dataset 
    name_mapping = 'order_num', 'employee_id','customer_id','product_id','sales_channel_id','currency_code','order_quantity','total_cost','total_price','order_date','ship_date','delivery_date','procure_date'  
    source = SQLSource(connection = sourceDatabase, query = sql, names = name_mapping)

    # Lookup the given row. If that fails, insert it. If found, see if values for attributes in otherrefs or measures have changed and update the found row 
    # if necessary (note that values for attributes in keyrefs are not allowed to change). If an update is necessary and a factexpander is defined, 
    # the row will first be updated with any missing otherrefs/measures and the factexpander will be run on it.
    for row in source:
        asft.ensure(row)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()


def main():
    dim_stores(sourceDatabase, dw_conn_wrapper)
    dim_employees_scd1(sourceDatabase, dw_conn_wrapper)
    dim_customers(sourceDatabase, dw_conn_wrapper)
    dim_products(sourceDatabase, dw_conn_wrapper)
    dim_channels(sourceDatabase, dw_conn_wrapper)
    fact_orders_scd1(sourceDatabase, dw_conn_wrapper)
    fact_orders_acc(sourceDatabase, dw_conn_wrapper)


if __name__ == '__main__':
    # Connect to salesdb (OLTP) and salesdwh (OLT)
    sourceDatabase = MySQLdb.connect(database = 'salesdb', user = 'user', password = 'password', port = 42333)
    destDatabase = duckdb.connect(r'C:\Users\katep\OneDrive\Documents\Andreas\dimensional-data-modeling\assets_scripts\salesdwh.duckdb') # Change the path if you have your sales duckDB somewhere else
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection = destDatabase)
    main()
