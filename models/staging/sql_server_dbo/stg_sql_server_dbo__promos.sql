with 

source as (

    select * from {{ source('sql_server_dbo', 'promos') }}

),

renamed_and_recasted as (
    select
        md5(promo_id)::VARCHAR(32) AS promo_id,
        promo_id::VARCHAR(50) AS promo_description,
        discount::FLOAT AS discount_dollars,
        CASE
            WHEN status='active' THEN 1::Boolean
            else 0::Boolean
        END AS is_active,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1
    union all
    select
        'NO_PROMO' AS promo_id,
        'No promo' AS promo_description,
        0 AS discount_dollars,
        0::Boolean AS is_active,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP) AS last_loaded_utc
)

select * from renamed_and_recasted