{{
  config(
    materialized='table'
  )
}}

WITH stg_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
),

renamed_casted AS (
    SELECT
        event_id 
        , page_url
        , event_type
        , user_id
        , product_id
        , session_id
        , created_at AS created_at_utc
        , order_id
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status
        
    FROM stg_events
    )

SELECT * FROM renamed_casted