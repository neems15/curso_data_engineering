{{ config(
    materialized='incremental',
    unique_key = '_row' 
    ) 
    }}
----- UNIQUE_key --> SCD1. No tiene historificación. Se coge la PK
------ si hacemos dbt run full refresh carga la tabla de nuevo
--on_schema_change='fail' falla. Para meter los registros antiguos hacemos full resfresh
-- * **ignore**: Estado por defecto.
-- * **fail**: Devuelve mensaje de error cuando los schemas de la tabla origen y destino son distintos.
-- * **append_new_columns:** Agrega nuevas columnas a la tabla existente. No elimina columnas que ya existen.
-- * **sync_all_columns:** Agrega columnas a la tabla existente y elimina columnas que no sean necesarias.


WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget') }}

{% if is_incremental() %}

	  WHERE _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }} )

{% endif %}
    ),

renamed_casted AS (
    SELECT
          _row
        , month
        , quantity 
        , _fivetran_synced
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted