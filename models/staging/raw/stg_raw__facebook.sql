with 

source as (

    select * from {{ source('raw', 'facebook') }}

),

renamed as (

    select

    from source

)

select * from renamed