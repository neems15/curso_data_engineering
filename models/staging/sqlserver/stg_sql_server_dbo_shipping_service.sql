WITH stg_shipping_service AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_orders')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    MD5(shipping_service) AS shipping_id
    , shipping_service AS nombre
        
    FROM stg_shipping_service
    )

SELECT * FROM renamed_casted