{{ config(materialized='view') }}

with vw_countries as (

    select
    case
    WHEN contains(f.value:code, '[') then f.value:code[0]::string 
    WHEN not contains(f.value:code, '[') then f.value:code::string 
    end as code,
    f.value:name:content::string as name,
    f.value:uf as uf,
    CASE
    WHEN contains(f.value, 'status') then 'obsolete'
    WHEN not contains(f.value, 'status') then 'active'
    end as status

    from {{ ref('stg_countries') }} m,
    lateral flatten(input => data_json:codelist.countries.country) f
)

 select * from vw_countries
