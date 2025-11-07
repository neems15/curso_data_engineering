{{
  config(
    materialized='view'
  )
}}

WITH stg_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
),

renamed_casted AS (
    SELECT
        product_id 
        , price 
        , name
        , inventory
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status ----------------------------------??????
        
    FROM stg_products
    )

SELECT * FROM renamed_casted