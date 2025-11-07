with 

source as (

    select * from {{ source('sql_server_dbo', 'events') }}

),

renamed as (

    select
        event_id::VARCHAR(50) AS event_id,
        page_url::VARCHAR(200) AS page_url,
        event_type::VARCHAR(50) AS event_type,
        user_id::VARCHAR(50) AS user_id,
        product_id::VARCHAR(50) AS product_id,
        session_id::VARCHAR(50) AS session_id,
        CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', created_at) AS created_at, -- Assumes the timestamp was created in the default time zone
        order_id::VARCHAR(50) AS order_id,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1

)

select * from renamed