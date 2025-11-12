WITH stg_zipcodes AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_countries')  }}
),

renamed_casted AS (
    SELECT
        , md5(zipcode) as zipcode_id 
        , zipcode
        , md5(state) as state_id 
        , state
        , date_load
        , delete_status
        
    FROM stg_zipcodes

    )

SELECT * FROM renamed_casted