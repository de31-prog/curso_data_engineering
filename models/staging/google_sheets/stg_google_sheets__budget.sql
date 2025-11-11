{{ config(
    materialized='incremental',
    unique_key = '_row'
    ) 
    }}

WITH source AS (
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
        , CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    )

SELECT * FROM renamed_casted