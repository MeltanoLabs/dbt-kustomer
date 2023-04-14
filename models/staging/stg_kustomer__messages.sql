with source as (

    select * from {{ source('kustomer', 'messages') }}

)

, final as (

    select
        id as message_id
        , relationships:conversation:data:id as conversation_id
        , relationships:customer:data:id as customer_id
        , attributes:app as app
        , attributes:source as source
        , attributes:channel as channel
        , attributes:direction as direction
        , attributes:preview as preview
        , attributes:sentAt as sent_at
        , attributes:size as size
        , attributes:status as status
        , attributes:subject as subject
        , relationships:modifiedBy:data:id as modified_by
        , relationships:createdBy:data:id as created_by
        , updated_at

    from source

)

select * from final