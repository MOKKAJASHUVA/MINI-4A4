{{
    config(
        materialized='incremental'
    )
}}

with 
hub_data as (

    select * from {{ ref('accident_hub') }}
),
vehicle_data as
(   
    select * from {{ ref('stg_vehicle') }}
)

select 
    hub_accident_hash as vehicle_sat_hash ,
    ACCIDENT_YEAR ,
    accident_reference ,
    vehicle_reference ,
    vehicle_type,
    towing_and_articulation ,
    vehicle_manoeuvre ,
    vehicle_direction_from ,
    vehicle_direction_to ,
    vehicle_location_restricted_lane ,
    junction_location ,
    skidding_and_overturning ,
    hit_object_in_carriageway ,
    vehicle_leaving_carriageway ,
    hit_object_off_carriageway ,
    first_point_of_impact ,
    vehicle_left_hand_drive ,
    journey_purpose_of_driver ,
    sex_of_driver ,
    age_of_driver ,
    age_band_of_driver ,
    engine_capacity_cc ,
    propulsion_code ,
    age_of_vehicle ,
    generic_make_model ,
    driver_imd_decile ,
    driver_home_area_type ,
    inserted_by ,
    sat_eff_date 
from vehicle_data



{% if is_incremental() %}

  where SAT_EFF_DATE > (select coalesce(max(SAT_EFF_DATE),'2022-01-02 16:00:00 +00:00') from {{ this }})
  AND
  vehicle_sat_hash in ( select HUB_ACCIDENT_HASH from hub_data )

{% endif %}