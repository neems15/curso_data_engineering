WITH stg_zipcodes AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_countries')  }}
),

renamed_casted AS (
    SELECT
        MD5(zipcode_id) as zipcode_id 
        , zipcode AS zipcode_name
        , MD5(state) as state_id 
        , state
        , date_load
        , delete_status
        
    FROM stg_zipcodes

    )

SELECT * FROM renamed_casted