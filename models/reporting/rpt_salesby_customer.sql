{{config(materialized='view', schema='reporting')}}

select 
c.company_name, c.contact_name,
sum(o.linesaleamount) as total_sales,
avg(o.margin) as avg_margin,min(o.orderdate) as first_orderdate,
max(o.orderdate) as last_orderdate,
count(o.orderid) as total_orders
from {{ref('dim_customer')}} as c inner join
{{ref('fct_orders')}} as o on c.customer_id=o.customerid
--where c.city={{var ('vcity',"'London'")}}
group by c.company_name,c.contact_name--c.city
order by total_sales desc


