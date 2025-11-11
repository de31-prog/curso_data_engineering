{{
    config(
        materialized='incremental'
    )
}}

with 

source as (

    select * from {{ source('sql_server_dbo', 'orders') }}
    {% if is_incremental() %}

    where _fivetran_synced > (select max(last_loaded_utc) from {{this}})

    {% endif %}

),

renamed as (
    select
        order_id::VARCHAR(50) AS order_id,
        SHIPPING_SERVICE::VARCHAR(20) AS shipping_service,
        replace(SHIPPING_COST, ',', '.')::FLOAT AS shipping_cost_dollars,
        ADDRESS_ID::VARCHAR(50) AS address_id,
        CONVERT_TIMEZONE('UTC', created_at) AS created_at,
        CASE
            WHEN promo_id is null OR promo_id ='' THEN 'NO_PROMO'::VARCHAR(32)
            ELSE md5(promo_id)::VARCHAR(32)
        END AS promo_id,
        CONVERT_TIMEZONE('UTC', estimated_delivery_at) AS estimated_delivery_at,
        replace(ORDER_COST, ',', '.')::FLOAT as order_cost_dollars,
        USER_ID::VARCHAR(50) AS user_id,
        replace(ORDER_TOTAL, ',', '.')::FLOAT AS order_total_dollars,
        CONVERT_TIMEZONE('UTC', delivered_at) AS delivered_at,
        TRACKING_ID::VARCHAR(50) AS tracking_id,
        STATUS::VARCHAR(20) AS status,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
)

select * from renamed