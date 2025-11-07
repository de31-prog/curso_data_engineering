with 

source as (

    select * from {{ source('sql_server_dbo', 'order_items') }}

),

renamed as (
    select
        order_id::VARCHAR(50) AS order_id,
        product_id::VARCHAR(50) AS product_id,
        quantity::number(38, 0) AS quantity,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1
)

select * from renamed