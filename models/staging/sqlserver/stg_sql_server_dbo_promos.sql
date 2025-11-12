
WITH stg_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
),

renamed_casted AS (
    SELECT
        promo_id::varchar(50) AS promo_name
        , md5 (promo_id) AS promo_id --te devuelve lo mismo, en orders la aplicamos igual para mantenener la idempotencia 
        , discount::float AS discount_usd
        , (CASE 
                WHEN status = 'active' THEN TRUE
                ELSE FALSE
            END)::varchar(50) AS IsActive,
        , convert_timezone ('UTC', _fivetran_synced) AS date_load
        , _fivetran_deleted AS delete_status ------------------------------------??
        
    FROM stg_promos

    UNION ALL
     
    SELECT 
        'no_promo' AS promo_name,
        md5('no_promo') AS promo_id,
        0 AS discount_usd,
        'active' AS status,
        current_timestamp() AS date_load,
        null AS delete_status

    )

SELECT * FROM renamed_casted



--y despues en orders tenemos que hacer que aparezca el promoid y el no promo 

-- tipar columnas o en sql o en contratos

--test relantionships para ver la integridad referencial