WITH stg_countries AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_addresses')  }}
),

renamed_casted AS (
    SELECT DISTINCT
        md5(country) as country_id
        , country AS country_name
        , md5(zipcode) as zipcode_id 
        , zipcode
        , state --- coalesce??
        , date_load
        , delete_status
        
    FROM stg_countries

    )

SELECT * FROM renamed_casted
