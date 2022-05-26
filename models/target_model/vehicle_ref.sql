with vehicle_ref_cleaning as 
(
    select * from {{ source('target_src', 'accident_ref') }}
       where table_n like 'Vehicle' and
          code_value is not null and
          label is not null
),
vehicle_ref as 
(   select
      cast(field_name as varchar(200)) as field_name ,
      cast(code_value as number(4)) as code_value,
      cast(label as varchar(200)) as label
    from vehicle_ref_cleaning
)

select * from vehicle_ref