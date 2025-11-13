WITH stg_event_type AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_events') }}
),

renamed_casted AS (
    SELECT DISTINCT
        MD5(event_type_name) AS event_type_id,
        event_type_name AS event_type,
        date_load,
        delete_status
    FROM stg_event_type

    UNION ALL

    SELECT 
        MD5('no_type') AS event_type_id,
        'no_type' AS event_type,
        CURRENT_TIMESTAMP() AS date_load,
        FALSE AS delete_status
)

SELECT * FROM renamed_casted
