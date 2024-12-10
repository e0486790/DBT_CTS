{{config (materialized='table',schema = env_var('DBT_STAGESCHEMA','STAGE_DEV'))}}

select *
from 
{{ source('qwt_raw','products')}}