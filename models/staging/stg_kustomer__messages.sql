with source as (

    select * from {{ source('kustomer', 'messages') }}

)

, final as (

    select
        id as message_id
        , {{ extract_json_field('relationships', ['conversation', 'data', 'id']) }} as conversation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
        , {{ extract_json_field('attributes', ['app']) }} as app
        , {{ extract_json_field('attributes', ['source']) }} as source
        , {{ extract_json_field('attributes', ['channel']) }} as channel
        , {{ extract_json_field('attributes', ['direction']) }} as direction
        , {{ extract_json_field('attributes', ['preview']) }} as preview
        , {{ extract_json_field('attributes', ['sentAt']) }} as sent_at
        , {{ extract_json_field('attributes', ['size']) }} as size
        , {{ extract_json_field('attributes', ['status']) }} as status
        , {{ extract_json_field('attributes', ['subject']) }} as subject
        /* Meltano specific field */
        , updated_at

    from source

)

select * from final