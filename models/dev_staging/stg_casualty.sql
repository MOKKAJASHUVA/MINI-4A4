with casualty_data as
(
    select
        cast(ACCIDENT_INDEX as varchar(100)) as ACCIDENT_INDEX,
        cast(ACCIDENT_YEAR as  number(4)) as ACCIDENT_YEAR,
        cast(ACCIDENT_REFERENCE as varchar(100)) as ACCIDENT_REFERENCE,
        cast(vehicle_reference as number(6)) as vehicle_reference,
        cast( casualty_reference as number(5)) as  casualty_reference,
        cast( casualty_class  as number(1))as  casualty_class ,
        cast(sex_of_casualty  as number(1))as  sex_of_casualty ,
        cast(  age_of_casualty  as number(3))as   age_of_casualty  ,
        cast( age_band_of_casualty    as  number(2))as  age_band_of_casualty   ,
        cast(casualty_severity as number(1))as casualty_severity,
        cast( pedestrian_location   as  number(2))as  pedestrian_location   ,
        cast(pedestrian_movement as  number(1))as  pedestrian_movement,
        cast( car_passenger  as number(1))as  car_passenger ,
        cast(  bus_or_coach_passenger   as number(1))as  bus_or_coach_passenger  ,
        cast(  pedestrian_road_maintenance_worker   as number(1))as  pedestrian_road_maintenance_worker   ,
        cast( casualty_type  as  number(3))as  casualty_type ,
        cast(  casualty_imd_decile  as  number(2))as  casualty_imd_decile ,
        cast(  casualty_home_area_type   as number(2))as  casualty_home_area_type,
        cast( hub_accident_hash as varchar(200))as hub_accident_hash ,
        cast( inserted_by as varchar(200))as inserted_by,
        cast(SAT_EFF_DATE as timestamp) as  SAT_EFF_DATE
    from {{ source('accident_src', 'casualty_landing') }}
)

select * from casualty_data