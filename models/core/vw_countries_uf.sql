{{ config(materialized='view') }}

with vw_countries_uf as (

select v.code as code,
CASE
WHEN not contains(f.value, 'authorized') and contains(f.value, '{') then f.value:name::string
WHEN contains(f.value, 'authorized') and contains(f.value, 'name') then f.value:name:content::string
WHEN contains(f.value, 'authorized') and not contains(f.value, 'name') then f.value:content::string
WHEN not contains(f.value, '{') then f.value::string
WHEN contains(f.value, '[') then f.value:name::string 
else 'False'
end as uf

from {{ref('vw_countries')}} v,
lateral flatten(input => v.uf) f
)

select *
from vw_countries_uf