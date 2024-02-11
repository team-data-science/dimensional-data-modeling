import pandas as pd
import MySQLdb
import pygrametl
from pygrametl.datasources import CSVSource, SQLSource, PandasSource
from pygrametl.tables import Dimension, FactTable, SlowlyChangingDimension, TypeOneSlowlyChangingDimension, AccumulatingSnapshotFactTable
import duckdb

def dim_employees_scd2(sourceDatabase, dw_conn_wrapper):
    """The function creates the dimensional table - SCD type 2"""
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
    dw_conn_wrapper.close()

def main():
    dim_employees_scd2(sourceDatabase, dw_conn_wrapper)

if __name__ == '__main__':
    # Connect to salesdb (OLTP) and salesdwh (OLT)
    sourceDatabase = MySQLdb.connect(database = 'salesdb', user = 'user', password = 'password', port = 42333)
    destDatabase = duckdb.connect(r'C:\Users\katep\OneDrive\Documents\Andreas\dimensional-data-modeling\assets_scripts\salesdwh.duckdb') # Change the path if you have your sales duckDB somewhere else
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection = destDatabase)
    main()