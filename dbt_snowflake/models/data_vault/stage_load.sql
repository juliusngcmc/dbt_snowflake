with round_number as(
--     1. put round_number for every record partition by ID and order by _FIVETRAN_START
        SELECT *, row_number() OVER (PARTITION BY ID ORDER BY _FIVETRAN_START DESC) AS round_num
        FROM FIVETRAN_DATABASE.RAW.APPOINTMENT
),
    existed as (
--     2. check if ID existed the day before
      select *,
             case
                 when exists(
                         select *
                         from FIVETRAN_DATABASE.RAW.APPOINTMENT
                         where ID = round_number.ID
                             and to_date(_FIVETRAN_START) < to_date(round_number._FIVETRAN_START)
                     )
                 then 1
                 else 0
                 end as existed
      from round_number
    ),
    lasted_record as(
--      3. take the lasted record for each ID
        select * from existed
             where round_num = 1
    ),
    operation as(
--      4. identify as DELETE or INSERT OR UPDATE
        select *,
               case
                   when _FIVETRAN_ACTIVE = 'TRUE' and existed = 1 then 'UPDATE'
                   when _FIVETRAN_ACTIVE = 'TRUE' and existed = 0 then 'INSERT'
                   else 'DELETED'
               end as OPERATION
        from lasted_record
    )
select ID                        as SOURCE_SYSTEM_ID
       ,to_date(_FIVETRAN_START) as LOAD_DATE
       ,OPERATION
       ,_FIVETRAN_START         as SOURCE_MODIFIED
from operation
order by ID desc