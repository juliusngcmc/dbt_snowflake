{{ airbyte_stage_load_macros(
    model_name = source('fivetran_database','invoice_airbyte')
)
}}