WITH stg_countries AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_addresses')  }}
),

renamed_casted AS (
    SELECT
        md5(country || zipcode || state) as country_id
        , zipcode 
        , country
        , state --- coalesce??
        , date_load
        , delete_status
        
    FROM stg_countries

    )

SELECT * FROM renamed_casted
