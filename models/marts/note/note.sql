with source as (

select * from {{ ref('stg_kustomer__notes') }}

)

, final as (

    select
        note_id as id
        , deleted_by_id as deleted_by
        , conversation_id
        , created_by_id as created_by
        , customer_id
        , modified_by_id as modified_by
        , body
        , deleted_by_id is not null as deleted
        , null as external_id

    from source

)

select * from final