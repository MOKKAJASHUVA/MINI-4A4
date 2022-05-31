{{
    config(
        materialized='incremental'
    )
}}

with 
hub_data as (

    select * from {{ ref('accident_hub') }}
),
accident_data as
(   
    select * from {{ ref('stg_accident') }}
)

select        
        hub_accident_hash as ACCIDENT_SAT_HASH,
        ACCIDENT_YEAR,
        ACCIDENT_REFERENCE,
        location_easting_osgr,
        LOCATION_NORTHING_OSGR,
        longitude,
        latitude,
        police_force,
        accident_severity,
        number_of_vehicles,
        number_of_casualties,
        date,
        day_of_week,
        time,
        local_authority_district,
        local_authority_ons_district,
        local_authority_highway,
        first_road_class,
        first_road_number,
        road_type,
        speed_limit,
        junction_detail,
        junction_control,
        second_road_class,
        second_road_number,
        pedestrian_crossing_human_control,
        pedestrian_crossing_physical_facilities,
        light_conditions,
        weather_conditions,
        road_surface_conditions,
        special_conditions_at_site,
        carriageway_hazards,
        urban_or_rural_area,
        did_police_officer_attend_scene_of_accident,
        trunk_road_flag,
        lsoa_of_accident_location,
        inserted_by,
        SAT_EFF_DATE
from accident_data 

{% if is_incremental() %}

  where SAT_EFF_DATE > (select coalesce(max(SAT_EFF_DATE),'2022-01-02 16:00:00 +00:00') from {{ this }})
  AND
  accident_sat_hash in ( select HUB_ACCIDENT_HASH from hub_data )

{% endif %}
