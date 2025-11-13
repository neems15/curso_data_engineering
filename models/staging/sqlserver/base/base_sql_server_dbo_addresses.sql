
WITH stg_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses')  }}
),

renamed_casted AS (
    SELECT
        md5(country)::VARCHAR(50) AS country_id 
        , md5(address_id) AS address_id
        , address_id AS address_name
        , zipcode::NUMBER(38,0) AS zipcode  
        , country::VARCHAR(50) AS country
        , address::VARCHAR(150) AS address
        , state::VARCHAR(50) AS state
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status  
        
    FROM stg_addresses
    )

SELECT * FROM renamed_casted
