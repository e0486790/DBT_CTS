{{config(materialized='view', schema='reporting')}}

select 
c.customer_id, c.contact_name,c.city,sum(o.linesaleamount) as sales ,
sum(o.quantity) as quantity, avg(o.margin) as margin
from {{ref('dim_customer')}} as c inner join
{{ref('fct_orders')}} as o on c.customer_id=o.customerid
where c.city={{var ('vcity',"'London'")}}
group by c.customer_id,c.contact_name,c.city
order by sales desc