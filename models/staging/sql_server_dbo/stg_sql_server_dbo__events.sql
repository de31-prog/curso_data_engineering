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
        session_id,
        created_at,
        order_id,
        _fivetran_deleted,
        _fivetran_synced

    from source
    where _fivetran_deleted != 1

)

select * from renamed