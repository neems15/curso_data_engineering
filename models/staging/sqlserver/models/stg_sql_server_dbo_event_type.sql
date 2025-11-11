WITH stg_event_type AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_events')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    md5(event_type) AS event_type_id
    , event_type AS event_type
    , date_load
    , delete_status  
        
    FROM stg_event_type

            UNION ALL
     
    SELECT 
        'no_type' AS event_type,
        md5('no_status') AS event_type_id,
    )

SELECT * FROM renamed_casted