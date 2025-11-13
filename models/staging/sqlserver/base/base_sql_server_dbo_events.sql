

WITH stg_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
),

renamed_casted AS (
    SELECT
        MD5(event_id)::VARCHAR(50) AS event_id
        , event_id::VARCHAR(50) AS event_name
        , page_url::VARCHAR(200) AS page_url
        , md5(event_type)::VARCHAR(50) AS event_type_id
        , event_type::VARCHAR(50) AS event_type_name 
        , user_id::VARCHAR(50) AS user_id
        , product_id::VARCHAR(50) AS product_id
        , session_id::VARCHAR(50) AS session_id
        , convert_timezone ('UTC', created_at)::TIMESTAMP_TZ(9) AS created_at_utc
        , order_id::VARCHAR(50) AS order_id
        , convert_timezone ('UTC', _fivetran_synced)::TIMESTAMP_TZ(9) AS date_load
        , _fivetran_deleted AS delete_status
        
    FROM stg_events
    )

SELECT * FROM renamed_casted
