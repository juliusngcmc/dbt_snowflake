select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('invoice_items_airbyte_stage_load') }}
except
select SOURCE_SYSTEM_ID, LOAD_DATE, OPERATION from {{ ref('invoice_items_fivetran_stage_load') }}