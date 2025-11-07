{{
  config(
    materialized='view'
  )
}}

WITH stg_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
),

renamed_casted AS (
    SELECT
        order_id 
        , user_id 
        , md5 (promo_id) AS promo_id
        , address_id
        , convert_timezone ('UTC', created_at) AS created_at_utc
        , item_order_cost_usd ----------------------------------------------------------------------------------------------------
        , shipping_cost AS shipping_cost_usd
        , order_total AS total_order_cost_usd
        , tracking_id
        , shipping_service ------------------------------------- A tabla nueva
        , convert_timezone ('UTC', estimated_delivery) AS estimated_delivery_at_utc 
        , convert_timezone ('UTC', delivered_at) AS delivered_at_utc 
				,	DATEDIFF(day, created_at_utc, delivered_at_utc) AS days_to_deliver
        , status_order -----------------------????
        , _fivetran_synced AS date_load
        , _fivetran_deleted AS delete_status -----------------------------??
    FROM stg_orders

    )

SELECT * FROM renamed_casted
--meter no promo en null 
