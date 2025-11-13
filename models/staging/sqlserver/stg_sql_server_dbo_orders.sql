

WITH source AS (

    SELECT * 
    FROM {{ ref('base_sql_server_dbo_orders') }}

),

renamed_casted AS (

    SELECT
        order_id 
        , user_id 
        , promo_id
        , address_id
        , created_at_utc
        , item_order_cost_usd
        , shipping_cost_usd
        , total_order_cost_usd
        , tracking_id
        , shipping_id
        , estimated_delivery_at_utc
        , delivered_at_utc
        , days_to_deliver
        , status_id
        , date_load
        , delete_status
    FROM source
)

SELECT * FROM renamed_casted
