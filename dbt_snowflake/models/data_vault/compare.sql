select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('appointment_airbyte_stage_load') }}
except
select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('appointment_fivetran_stage_load') }}
