{{config(materialized='table', schema = 'tarnsforming')}}

select
c.customer_id,
c.company_name,
c.contact_name,
c.city,
c.country,
d.divisionname,
c.address,
c.fax,
c.postal_code,
c.phone,
IFF(c.country = '', 'NA', c.state) as statename from 
{{ref('stg_customers')}} as c

inner join

{{ref('lkp_divisions')}} as d on c.division_id=d.divisionid