select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('invoice_airbyte_stage_load') }}
except
select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('invoice_fivetran_stage_load') }}