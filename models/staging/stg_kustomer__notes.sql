with source as (

    select * from {{ source('kustomer', 'notes') }}

)

, final as (

    select
        id as note_id
        , relationships:conversation:data:id as conversation_id
        , relationships:customer:data:id as customer_id
        , attributes:body as body
        , relationships:modifiedBy:data:id as modified_by
        , relationships:createdBy:data:id as created_by
        , updated_at

    from source

)

select * from final