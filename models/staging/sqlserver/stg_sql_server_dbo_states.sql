WITH stg_states AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_zipcodes')  }}
),

renamed_casted AS (
    SELECT DISTINCT
        MD5(state) as state_id 
        , state
        , date_load
        , delete_status
        
    FROM stg_states

    )

SELECT * FROM renamed_casted
