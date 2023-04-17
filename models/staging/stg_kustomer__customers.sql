with source as (

    select * from {{ source('kustomer', 'customers') }}

)

, final as (

    select
        id as customer_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by -- TODO always null
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
        , {{ extract_json_field('attributes', ['displayColor']) }} as display_color
        , {{ extract_json_field('attributes', ['displayName']) }} as display_name
        , {{ extract_json_field('attributes', ['externalId']) }} as external_id
        , {{ extract_json_field('attributes', ['gender']) }} as gender
        , {{ extract_json_field('attributes', ['locale']) }} as local
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['progressiveStatus']) }} as progressive_status
        , {{ extract_json_field('attributes', ['verified']) }} as verified
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , null as deleted_at -- TODO Find this field
        , {{ extract_json_field('attributes', ['lastActivityAt']) }} as last_activity_at
        , {{ extract_json_field('attributes', ['defaultLang']) }} as default_lang
        , {{ extract_json_field('attributes', ['deleted']) }} as deleted
        , {{ extract_json_field('attributes', ['firstName']) }} as first_name
        , {{ extract_json_field('attributes', ['lastName']) }} as last_name
        , {{ extract_json_field('attributes', ['lastMessageAt']) }} as last_message_at
        -- duplicate here in ERD of progressive_status
        -- duplicate here in ERD of verified
        , {{ extract_json_field('attributes', ['conversationCounts', 'open']) }} as conversation_count_open -- TODO check field name
        , {{ extract_json_field('attributes', ['conversationCounts', 'done']) }} as conversation_count_done -- TODO check field name
        , {{ extract_json_field('attributes', ['conversationCounts', 'snoozed']) }} as conversation_count_snoozed -- TODO check field name
        , {{ extract_json_field('attributes', ['conversationCounts', 'all']) }} as conversation_count_all -- TODO check field name
        , {{ extract_json_field('attributes', ['lastConversation', 'channels']) }} as last_conversation_channels -- TODO check field name
        , {{ extract_json_field('attributes', ['lastConversation', 'tags']) }} as last_conversation_tags -- TODO check field name
        , {{ extract_json_field('attributes', ['lastMessageOut', 'sentAt']) }} as last_message_out_sent_at -- TODO check field name
        -- sentiment_* -- TODO Add fields here
        /* Meltano specific field */
        , updated_at

    from source

)

select * from final
