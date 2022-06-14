{{
    config(
        materialized='incremental'
    )
}}

select
    cast(HUB_ACCIDENT_HASH as varchar(100)) as HUB_ACCIDENT_HASH,
    cast(ACCIDENT_INDEX as varchar(200)) as ACCIDENT_INDEX,
    cast(SAT_EFF_DATE as timestamp) as HUB_LOAD_DATE,
    cast(INSERTED_BY as varchar(100)) as HUB_INSERTED_BY
from {{ ref('stg_accident') }}

{% if is_incremental() %}

  where 
  (select count(HUB_ACCIDENT_HASH) from {{ this }}) = 0 
  OR 
  (HUB_ACCIDENT_HASH not in (select HUB_ACCIDENT_HASH from {{ this }}))

{% endif %}
