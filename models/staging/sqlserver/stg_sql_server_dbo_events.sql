

WITH stg_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
),

renamed_casted AS (
    SELECT
        event_id::VARCHAR(50) 
        , page_url::VARCHAR(200)
        , event_type::VARCHAR(50)
        , user_id::VARCHAR(50)
        , product_id::VARCHAR(50)
        , session_id::VARCHAR(50)
        , convert_timezone ('UTC', created_at)::TIMESTAMP_TZ(9) AS created_at_utc
        , order_id::VARCHAR(50)
        , convert_timezone ('UTC', _fivetran_synced)::TIMESTAMP_TZ(9) AS date_load
        , _fivetran_deleted AS delete_status ---------------------------------???
        
    FROM stg_events
    )

SELECT * FROM renamed_casted

