with 

source as (

    select * from {{ source('sql_server_dbo', 'addresses') }}

),

renamed_and_recasted as (

    select
        address_id::VARCHAR(50) AS address_id,
        zipcode::NUMBER(38,0) AS zipcode,
        country::VARCHAR(50) AS country,
        address::VARCHAR(150) AS address,
        state::VARCHAR(50) AS state,
        CONVERT_TIMEZONE('UTC', _fivetran_synced) AS last_loaded_utc
    from source
    where _fivetran_deleted != 1

)

select * from renamed_and_recasted