{{
    config(
        materialized='incremental'
    )
}}

with location_mapping_ref as (
    select 
        cast(WARD_CODE as varchar(100)) as WARD_CODE,
        cast(WARD_NAME as varchar(100)) as WARD_NAME,
        cast(LAD_CODE as varchar(100)) as LAD_CODE,
        cast(LAD_NAME as varchar(100)) as LAD_NAME,
        cast(REGION_CODE as varchar(100)) as REGION_CODE,
        cast(REGION_NAME as varchar(100)) as REGION_NAME,
        cast(COUNTRY_CODE as varchar(100)) as COUNTRY_CODE,
        cast(COUNTRY_NAME as varchar(100)) as COUNTRY_NAME,
        cast(location_hash as varchar(300))as location_hash ,
        cast(inserted_by as varchar(200))as inserted_by,
        cast(load_eff_date as timestamp) as  load_eff_date
    from {{ source('target_src', 'location_mapping') }}
)
select * from location_mapping_ref

{% if is_incremental() %}

  where load_eff_date > (select coalesce(max(load_eff_date),'2022-01-02 16:00:00 +00:00') from {{ this }}) 
  AND 
  (location_hash not in (select location_hash from {{ this }}))

{% endif %}
