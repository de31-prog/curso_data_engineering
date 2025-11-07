with 

source as (

    select * from {{ source('sql_server_dbo', 'products') }}

),

renamed_and_recasted as (

    select
        product_id::VARCHAR(50) AS product_id,
        REPLACE(price, ',', '.')::FLOAT AS price_dollars,
        name::VARCHAR(100) AS name,
        inventory::NUMBER(38, 0) AS inventory,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1

)

select * from renamed_and_recasted