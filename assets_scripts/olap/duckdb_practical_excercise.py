#importing libraries
import duckdb 
import pandas as pd

#creating a dataframe
data = {'employee_id':  ['1', '2'],
        'employee_name': ['Bernardo Figeroa', 'Ammie Corrio']
       }

df = pd.DataFrame(data)

#querying the table using duckdb
duckdb.sql("SELECT * from df")

# Change the path if you have your sales duckDB somewhere else
db = duckdb.connect(r'C:\Users\katep\OneDrive\Desktop\DEV-modeling\assets_scripts\test.duckdb')