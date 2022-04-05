{{ config(materialized='view') }}

with stg_countries as (

    select data, raw.XML2JSON(data) as data_json from raw.countries
)

select data_json 
from stg_countries