

WITH stg_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        user_id::VARCHAR(256)  
        , first_name::VARCHAR(256)  
        , last_name::VARCHAR(256)  
        , email::VARCHAR(256)  
        , phone_number::VARCHAR(256)  
        , convert_timezone ('UTC', created_at) AS created_at_utc
        , convert_timezone ('UTC', updated_at) AS updated_at_utc
        , address_id::VARCHAR(256)  
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted::BOOLEAN AS delete_status
    FROM stg_users
    )

SELECT * FROM renamed_casted