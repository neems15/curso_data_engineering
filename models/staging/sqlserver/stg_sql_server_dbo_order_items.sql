

WITH stg_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
),

renamed_casted AS (
    SELECT
        order_id::VARCHAR(50) 
        , user_id::VARCHAR(50)
        , product_id::VARCHAR(50)
        , quantity::NUMBER
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted::BOOLEAN AS delete_status
        
    FROM stg_order_items
    )

SELECT * FROM renamed_casted