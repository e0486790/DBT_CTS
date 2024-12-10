{{config(materialized='table', schema = 'tarnsforming')}}

select 
emp.empid,
emp.lastname,
emp.firstname,
emp.title,
emp.extension,
emp.year_salary,
emp.hire_date,
iff(mgr.firstname=null,emp.firstname,mgr.firstname) as manager,
emp.reports_to,
office.officecity,
office.officecountry

from
{{ref('stg_employees')}} as emp
left join
{{ ref('stg_employees')}} as mgr
on emp.reports_to=mgr.empid
inner join
{{ref('lkp_office')}} as office
on emp.office=office.office