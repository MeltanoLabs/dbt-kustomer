with source as (

    select * from {{ source('kustomer', 'notes') }}

)

, final as (

    select
        id as note_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['body']) }} as body
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['createdByTeams']) }} as created_by_teams

        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id
        , {{ extract_json_field('relationships', ['conversation', 'data', 'id']) }} as conversation_id

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final