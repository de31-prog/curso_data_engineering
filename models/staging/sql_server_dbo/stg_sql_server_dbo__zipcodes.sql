with 

source as (

    select * from {{ ref('base_sql_server_dbo__addresses') }}

),

renamed_and_recasted as (

    select
        md5(CONCAT(zipcode, state, country)) AS zipcode_id,
        zipcode::NUMBER(38,0) AS zipcode,
        country::VARCHAR(50) AS country,
        state::VARCHAR(50) AS state,
        last_loaded_utc
    from source
)

select * from renamed_and_recasted