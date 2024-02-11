--SCD Type 0, 1
--duckdb dwh
select count(*) as num, 'customer' as tbl from dim_customer
union all
select count(*) as num, 'product' as tbl from dim_product
union all
select count(*) as num, 'sales' as tbl from fact_sales_scd1;

--duckdb dwh
select 
f.order_num,
f.customer_id,
f.employee_id,
f.product_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
st.city_name,
st."type" ,
f.order_date,
f.order_quantity,
f.total_cost,
f.total_price
from fact_sales_scd1  f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd1 e on f.employee_id  = e.employee_id 
left join dim_product p on f.product_id  = p.product_id
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 000207';


--updating city name
--mysql
update city
set city_name = 'TestCity'
where city_name = 'Lexington-Fayette'

--updating type
--mysql
update city
set type = 'Town'
where city_name = 'TestCity'

--duckdb dwh
select customer_id, customer_name 
from dim_customer
where customer_id = '13';

--updating a customer record
--mysql
update customer
set customer_first_name = 'Test_First', customer_last_name = 'Test_Last'
where customer_id = '13';

--inserting a customer record to a sales transactional database
--mysql
insert into customer (customer_id, customer_first_name, customer_last_name)
values ('51', 'NewRecord_First', 'NewRecord_Last');

--duckdb dwh
select customer_id, customer_name 
from dim_customer
where customer_id = '51';


--Transactional Fact
--duckdb dwh
select 
f.order_num,
f.customer_id,
f.employee_id,
f.product_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
st.city_name,
st."type" ,
f.order_date,
f.order_quantity,
f.total_cost,
f.total_price
from fact_sales_scd1  f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd1 e on f.employee_id  = e.employee_id 
left join dim_product p on f.product_id  = p.product_id
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 000207';


--Accumulating snapshot Fact
--duckdb dwh
select 
f.order_num,
f.order_date,
f.ship_date,
f.shipment_lag 
from main.fact_sales_acc f
where f.order_num = 'SO - 000207';

--duckdb dwh
select count(*) as num from main.fact_sales_acc;

--mysql
update sales_order
set ship_date = '2023-01-01'
where order_num = 'SO - 000207';

--SCD type 2
--duckdb dwh
select 
f.order_num,
f.customer_id,
f.employee_id,
f.product_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
st.city_name,
st."type" ,
f.order_date,
f.order_quantity,
f.total_cost,
f.total_price
from fact_sales_scd1  f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd1 e on f.employee_id  = e.employee_id 
left join dim_product p on f.product_id  = p.product_id
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 000207';


--duckdb dwh
select count(*) as num from main.fact_sales_scd2;

--duckdb dwh
select * from dim_employee_scd2 
where employee_id  = '1'

--duckdb dwh
update dim_employee_scd2
set valid_from = '2018-05-30'

--duckdb dwh
select 
f.order_num,
f.employee_sk,
e.employee_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
s.sales_channel_name,
st.location as store_location,
f.order_quantity,
f.total_cost,
f.total_price,
f.order_date
from fact_sales_scd2 f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd2 e on f.employee_sk  = e.employee_sk 
left join dim_product p on f.product_id  = p.product_id
left join dim_sales_channel s on f.sales_channel_id  = s.sales_channel_id 
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 000207';

--mysql
update employee
set store_id = '50'
where employee_id  = '1';

--duckdb dwh
select 
f.order_num,
f.employee_sk,
e.employee_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
s.sales_channel_name,
st.location as store_location,
f.order_quantity,
f.total_cost,
f.total_price,
f.order_date
from fact_sales_scd2 f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd2 e on f.employee_sk  = e.employee_sk 
left join dim_product p on f.product_id  = p.product_id
left join dim_sales_channel s on f.sales_channel_id  = s.sales_channel_id 
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 000207';

--mysql
insert into sales_order (order_num, order_date, currency_code, order_quantity, discount_applied, ship_date,
delivery_date, procure_date, total_cost, total_price, employee_id, customer_id, sales_channel_id, product_id) values 
('SO - 15', current_date, 'USD', 1, 1,  '2023-07-24',  '2023-07-24',  '2023-07-24', 10, 10, '1', '1', '1', 1);

--duckdb dwh
select 
f.order_num,
f.employee_sk,
e.employee_id,
e.store_id,
c.customer_name,
e.employee_name,
p.product_name,
s.sales_channel_name,
st.location as store_location,
f.order_quantity,
f.total_cost,
f.total_price,
f.order_date
from fact_sales_scd2 f
left join dim_customer c on f.customer_id = c.customer_id 
left join dim_employee_scd2 e on f.employee_sk  = e.employee_sk 
left join dim_product p on f.product_id  = p.product_id
left join dim_sales_channel s on f.sales_channel_id  = s.sales_channel_id 
left join dim_store st on e.store_id = st.store_id 
where f.order_num = 'SO - 15';
