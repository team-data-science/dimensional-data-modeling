#importing libraries
import duckdb
from sql_queries_salesdb_olap import create_table_queries

def create_tables(dest):
    for query in create_table_queries:
        dest.execute(query)
        dest.commit()

def main():
    dest = destDatabase.cursor()
    create_tables(dest)

if __name__ == "__main__":
    destDatabase = duckdb.connect(r'C:\Users\katep\OneDrive\Documents\Andreas\dimensional-data-modeling\assets_scripts\salesdwh.duckdb') # Change the path if you have your sales duckDB somewhere else
    main()
