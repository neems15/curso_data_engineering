WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo_addresses')  }}
),

renamed_casted AS (
    SELECT
        md5(address_id) AS address_id
        , address_id AS address_name
        , address
        , date_load
        , delete_status  
        
    FROM stg_addresses
    )

SELECT * FROM renamed_casted