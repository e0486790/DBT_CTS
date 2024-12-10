{{ config(materialized = 'table' ,schema = env_var('DBT_STAGESCHEMA','STAGE_DEV'))}}

select
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
split_part(shipmentdate,' ', 0) as shipmentdate,
status
from 
{{ source('qwt_raw','shipments')}}