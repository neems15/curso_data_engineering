
WITH stg_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
),

renamed_casted AS (
    SELECT
        product_id::VARCHAR(256)  
        , price::FLOAT 
        , name::VARCHAR(256)  
        , inventory::NUMBER(38,0)
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted::BOOLEAN AS delete_status ----------------------------------??????
        
    FROM stg_products
    )

SELECT * FROM renamed_casted