WITH stg_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
        user_id
        , first_name
        , last_name
        , email
        , phone_number
        , created_at_utc
        , updated_at_utc
        , address_id
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status
    FROM stg_users
    )

SELECT * FROM renamed_casted