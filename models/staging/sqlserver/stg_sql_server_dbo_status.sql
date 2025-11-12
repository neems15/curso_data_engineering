WITH stg_status AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_orders')  }}
),


renamed_casted AS (
    SELECT DISTINCT
    md5(status) AS status_id
    , status
        
    FROM stg_status

        UNION ALL
     
    SELECT 
        'no_status' AS status,
        md5('no_status') AS status_id,
    )

SELECT * FROM renamed_casted