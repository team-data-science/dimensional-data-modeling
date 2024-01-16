
create_stores=('''CREATE TABLE IF NOT EXISTS dim_store (
      store_id varchar(50),
      latitude DECIMAL(18, 2),
      longitude DECIMAL(18, 2),
      location varchar(255),
      city_name varchar(255),
      type varchar(45),
      area_code varchar(45),
      state_code varchar(45),
      state varchar(255)) ''')    

create_employees_seq=('''CREATE SEQUENCE employee_sk START 1;''')
create_employees_scd2=('''CREATE TABLE IF NOT EXISTS dim_employee_scd2 (
      employee_id VARCHAR(50),
      employee_name VARCHAR(255),
      valid_from DATE,
      valid_to DATE,
      store_id VARCHAR(50),
      version integer,
      employee_sk  integer primary key default nextval('employee_sk')) ''')
                       
create_employees_scd1=('''CREATE TABLE IF NOT EXISTS dim_employee_scd1 (
      employee_id VARCHAR(50) ,
      employee_name VARCHAR(255),
      store_id VARCHAR(50))''')

create_customers=('''CREATE TABLE IF NOT EXISTS dim_customer (
      customer_id varchar(50),
      customer_name varchar(255))''')    

create_products=('''CREATE TABLE IF NOT EXISTS dim_product (
      product_id varchar(50) ,
      product_name varchar(255))''')  

create_channels=('''CREATE TABLE IF NOT EXISTS dim_sales_channel (
      sales_channel_id varchar(50),
      sales_channel_name varchar(255))''')

create_sales_scd2=('''CREATE TABLE IF NOT EXISTS fact_sales_scd2 (
      order_num varchar(50),
      employee_sk integer,
      employee_id varchar(50),
      customer_id varchar(50),
      product_id varchar(50),
      sales_channel_id varchar(50),
      currency_code varchar(45),
      order_quantity DECIMAL(18, 2),
      total_cost DECIMAL(18, 2),
      total_price DECIMAL(18, 2),
      order_date date,
      ship_date date,
      delivery_date date,
      procure_date date)''')

create_sales_scd1=('''CREATE TABLE IF NOT EXISTS fact_sales_scd1 (
      order_num varchar(50),
      employee_id varchar(50),
      customer_id varchar(50),
      product_id varchar(50),
      sales_channel_id varchar(50),
      currency_code varchar(45),
      order_quantity DECIMAL(18, 2),
      total_cost DECIMAL(18, 2),
      total_price DECIMAL(18, 2),
      order_date date,
      ship_date date,
      delivery_date date,
      procure_date date)''')

create_sales_acc=('''CREATE TABLE IF NOT EXISTS fact_sales_acc (
      order_num varchar(50),
      employee_id varchar(50),
      customer_id varchar(50),
      product_id varchar(50),
      sales_channel_id varchar(50),
      currency_code varchar(15),
      order_quantity DECIMAL(18, 2),
      total_cost DECIMAL(18, 2),
      total_price DECIMAL(18, 2),
      shipment_lag integer,
      delivery_lag integer,
      order_date date,
      ship_date date,
      delivery_date date,
      procure_date date)''')


create_table_queries = [create_stores, create_employees_seq, create_employees_scd2, create_employees_scd1, create_customers, create_products, create_channels, create_sales_scd2, create_sales_scd1, create_sales_acc]
