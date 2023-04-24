with source as (

select * from {{ ref('stg_kustomer__tags') }}

)

, final as (

    select
        tag_id as id
        , created_by_id as created_by
        , deleted_by_id as deleted_by
        , modified_by_id as modified_by
        , color
        , name

    from source

)

select * from final