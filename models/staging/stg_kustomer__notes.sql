with source as (

    select * from {{ source('kustomer', 'notes') }}

)

, final as (

    select
        id as note_id
        , {{ extract_json_field('relationships', ['conversation', 'data', 'id']) }} as conversation_id
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('attributes', ['body']) }} as body
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by
        , updated_at

    from source

)

select * from final