{{
    config(
        materialized='incremental'
    )
}}

with 
hub_data as (

    select * from {{ ref('accident_hub') }}
),
casualty_data as
(   
    select * from {{ ref('stg_casualty') }}
)

select 
    hub_accident_hash as casualty_sat_hash,
    accident_year,
    accident_reference,
    vehicle_reference,
    casualty_reference,
    casualty_class,
    sex_of_casualty,
    age_of_casualty,
    age_band_of_casualty,
    casualty_severity,
    pedestrian_location,
    pedestrian_movement,
    car_passenger,
    bus_or_coach_passenger,
    pedestrian_road_maintenance_worker,
    casualty_type,
    casualty_imd_decile,
    casualty_home_area_type,
    inserted_by ,
    sat_eff_date 
from casualty_data

{% if is_incremental() %}

  where SAT_EFF_DATE > (select coalesce(max(SAT_EFF_DATE),'2022-01-02 16:00:00 +00:00') from {{ this }})
  AND
  casualty_sat_hash in ( select HUB_ACCIDENT_HASH from hub_data )

{% endif %}