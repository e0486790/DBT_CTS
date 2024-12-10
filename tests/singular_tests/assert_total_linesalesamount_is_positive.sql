select 
orderid,
sum(linesalesamount) as tot_sales
 from
  {{ref('fct_order')}}
 group by orderid
 having tot_sales < 0