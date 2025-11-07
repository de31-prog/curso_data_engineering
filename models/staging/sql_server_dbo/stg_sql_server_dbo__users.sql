with 

source as (

    select * from {{ source('sql_server_dbo', 'users') }}

),

renamed as (

    select
        user_id::VARCHAR(50) AS user_id,
        CONVERT_TIMEZONE('UTC', updated_at) AS updated_at,
        address_id::VARCHAR(50) AS address_id,
        last_name::VARCHAR(50) AS last_name,
        CONVERT_TIMEZONE('UTC', created_at) AS created_at,
        phone_number::VARCHAR(20) AS phone_number,
        first_name::VARCHAR(50) AS first_name,
        email::VARCHAR(100) AS email,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1
)

select * from renamed