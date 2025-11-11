{{ 
    config(
        materialized = 'incremental',
        unique_key = 'order_id'
    ) 
}}

WITH source AS (

    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}

    {% if is_incremental() %}
        WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }})
    {% endif %}

),

renamed_casted AS (

    SELECT
        order_id::VARCHAR(256) AS order_id
        , user_id::VARCHAR(256) AS user_id
        , (CASE 
            WHEN promo_id IS NULL THEN 'no_promo' 
            ELSE MD5(LOWER(REPLACE(REPLACE(promo_id, ' ', ''), '-', '')))
        END)::VARCHAR(50) AS promo_id
        , address_id::VARCHAR(256) AS address_id
        , convert_timezone('UTC', created_at) AS created_at_utc
        , order_cost::FLOAT AS item_order_cost_usd
        , shipping_cost::FLOAT AS shipping_cost_usd
        , order_total::FLOAT AS total_order_cost_usd
        , tracking_id::VARCHAR(256) AS tracking_id
        , md5(shipping_service)::VARCHAR(256) AS shipping_id -------------------------------------
        , convert_timezone('UTC', estimated_delivery_at) AS estimated_delivery_at_utc
        , convert_timezone('UTC', delivered_at) AS delivered_at_utc
        , DATEDIFF(day, created_at, delivered_at) AS days_to_deliver
        , md5(status)::VARCHAR(256) AS status_id
        , convert_timezone('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted::BOOLEAN AS delete_status
    FROM source
)

SELECT * FROM renamed_casted
