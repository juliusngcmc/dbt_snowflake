{{ fivetran_stage_load_macros(
    model_name = source('fivetran_database','appointment_fivetran')
)
}}