{% macro airbyte_stage_load_macros(model_name) %}
with round_number as(
--     1. put round_number for every record partition by ID and order by _FIVETRAN_START
        SELECT *, row_number() OVER (PARTITION BY ID ORDER BY _AB_CDC_UPDATED_AT DESC) AS round_num
        FROM {{ model_name }}
)
    ,existed as (
--     2. check if ID existed the day before
      select *,
             case
                 when exists(
                         select *
                         from {{ model_name }}
                         where ID = round_number.ID
                             and to_date(_AB_CDC_UPDATED_AT) < to_date(round_number._AB_CDC_UPDATED_AT)
                     )
                 then 1
                 else 0
                 end as existed
      from round_number
    )
    ,lasted_record as(
--      3. take the lasted record for each ID
        select * from existed
             where round_num = 1
    )
    ,operation as(
--      4. identify as DELETE or INSERT OR UPDATE
        select *,
               case
                   when _AB_CDC_DELETED_AT is null and existed = 1 then 'UPDATE'
                   when _AB_CDC_DELETED_AT is null and existed = 0 then 'INSERT'
                   else 'DELETED'
               end as OPERATION
        from lasted_record
    )
select ID                           as SOURCE_SYSTEM_ID
       ,to_date(_AB_CDC_UPDATED_AT) as LOAD_DATE
       ,OPERATION
       ,_AB_CDC_UPDATED_AT          as SOURCE_MODIFIED
from operation order by ID desc

{% endmacro %}

