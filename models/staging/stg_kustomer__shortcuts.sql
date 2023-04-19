with source as (

    select * from {{ source('kustomer', 'shortcuts') }}

)

, final as (

    select
        id as note_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['draft', 'text']) }} as draft_text
        , {{ extract_json_field('attributes', ['conversation', 'custom']) }} as conversation_custom -- JSON object with unknown keys
        , {{ extract_json_field('attributes', ['payload']) }} as payload -- JSON object
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['isPrivate']) }} as is_private
        , {{ extract_json_field('attributes', ['deleted']) }} as deleted
        , {{ extract_json_field('attributes', ['rev']) }} as rev
        , {{ extract_json_field('attributes', ['appDisabled']) }} as app_disabled

        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id
        , {{ extract_json_field('relationships', ['parent', 'data', 'id']) }} as parent_id
        , {{ extract_json_field('relationships', ['accessUsers']) }} as access_users
        , {{ extract_json_field('relationships', ['accessTeams']) }} as access_teams
        , {{ extract_json_field('relationships', ['attachments', 'data']) }} as attachments -- JSON array of objects

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final