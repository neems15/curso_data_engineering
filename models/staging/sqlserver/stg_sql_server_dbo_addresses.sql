{{
  config(
    materialized='table'
  )
}}

WITH stg_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses')  }}
),

renamed_casted AS (
    SELECT
        address_id 
        , zipcode 
        , country
        , address
        , state
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status
        
    FROM stg_addresses
    )

SELECT * FROM renamed_casted