with accident_ref_cleaning as
(
  select * from {{ source('target_src', 'accident_ref') }} 
       where table_n like 'Accident' and
          code_value is not null and
          label is not null
),
accident_ref as
(   select
      cast(field_name as varchar(200)) as field_name ,
      cast(code_value as varchar(100)) as code_value,
      cast(label as varchar(200)) as label
    from accident_ref_cleaning
)

select * from accident_ref

