WITH stg_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
),

renamed_casted AS (
    SELECT
        promo_id::VARCHAR(50) AS promo_name,
        MD5(promo_id)::VARCHAR(50) AS promo_id,  -- mantiene la idempotencia
        discount::FLOAT AS discount_usd,
        (CASE 
            WHEN status = 'active' THEN TRUE 
            ELSE FALSE 
        END) AS is_active,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::TIMESTAMP_TZ(9) AS date_load,
        _fivetran_deleted::BOOLEAN AS delete_status
    FROM stg_promos

    UNION ALL

    SELECT 
        'no_promo' AS promo_name,
        MD5('no_promo')::VARCHAR(50) AS promo_id,
        0.0 AS discount_usd,
        TRUE AS is_active,
        CURRENT_TIMESTAMP() AS date_load,
        FALSE AS delete_status
)

SELECT * FROM renamed_casted
