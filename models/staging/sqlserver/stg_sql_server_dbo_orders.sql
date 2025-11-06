{{
  config(
    materialized='table'
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
        , created_at_utc
        , item_order_cost_usd
        , shipping_cost_usd
        , total_order_cost_usd
        , tracking_id
        , shipping_service
        , estimated_delivery_at_utc -- hay que hacer transformación de la hora a UTC
        , delivered_at_utc -- hay que hacer transformación de la hora a UTC
				,	DATEDIFF(day, created_at_utc, delivered_at_utc) AS days_to_deliver
        , status_order
        , _fivetran_synced AS date_load
        , _fivetran_deleted AS delete_status
    FROM stg_orders

    )

SELECT * FROM renamed_casted

 --UNION ALL

    --SELECT promo_name

    --FROM stg_sql_server_db_promos