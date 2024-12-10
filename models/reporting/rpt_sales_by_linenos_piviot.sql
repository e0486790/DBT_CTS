{{config(materialized='view', schema='reporting')}}

{% set linenos =  get_linenos() %}---using the macro name
 
select
orderid,
 {% for lno in linenos %} 
sum(case when lineno = {{lno}} then LINESALEAMOUNT end) as lineno{{lno}}_sales,
 {% endfor %} 
sum(LINESALEAMOUNT) as total_sales
from {{ ref('fct_orders') }}
group by 1