with source as (

    select * from {{ ref('stg_kustomer__teams') }}

)

, final as (

    select
        team_id as id
        , created_by_id as created_by
        , deleted_by_id as deleted_by
        , modified_by_id as modified_by
        , deleted
        , display_name
        , name

    from source

)

select * from final