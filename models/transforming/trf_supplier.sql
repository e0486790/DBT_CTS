{{config(materialized='table', schema = 'tarnsforming')}}


select 
GET(XMLGET(supplierinfo,'SupplierID'),'$')as SupplierId,
GET(XMLGET(supplierinfo,'CompanyName'),'$'):: varchar as Companyname,
GET(XMLGET(supplierinfo,'ContactName'),'$'):: varchar as Contactname,
GET(XMLGET(supplierinfo,'Address'),'$'):: varchar as Address,
GET(XMLGET(supplierinfo,'PostalCode'),'$'):: varchar as Postalcode,
GET(XMLGET(supplierinfo,'Country'),'$'):: varchar as Country,
GET(XMLGET(supplierinfo,'Phone'),'$'):: varchar as Phone,
GET(XMLGET(supplierinfo,'Fax'),'$'):: varchar as Fax
from
{{ref('stg_supplier')}} 