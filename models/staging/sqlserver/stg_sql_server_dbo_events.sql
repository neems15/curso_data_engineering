WITH base_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_events') }}
),

renamed_casted AS (
    SELECT
        MD5(event_id) AS event_id,
        page_url,
        event_type_id,
        user_id,
        product_id,
        session_id,
        created_at_utc,
        order_id,
        date_load,
        delete_status
    FROM base_events
)

SELECT * FROM renamed_casted
