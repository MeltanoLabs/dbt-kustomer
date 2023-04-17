with source as (

    select * from {{ source('kustomer', 'customers') }}

)

, final as (

    select
        id as customer_id
        , {{ extract_json_field('attributes', ['externalId']) }} as external_id
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['firstName']) }} as first_name
        , {{ extract_json_field('attributes', ['lastName']) }} as last_name
        , {{ extract_json_field('attributes', ['displayName']) }} as display_name
        , {{ extract_json_field('attributes', ['gender']) }} as gender
        , {{ extract_json_field('attributes', ['defaultLang']) }} as default_language
        , {{ extract_json_field('attributes', ['locale']) }} as locale
        , {{ extract_json_field('attributes', ['displayColor']) }} as display_color
        , {{ extract_json_field('attributes', ['progressiveStatus']) }} as progressive_status
        , {{ extract_json_field('attributes', ['deleted']) }} as is_deleted
        , {{ extract_json_field('attributes', ['verified']) }} as is_verified
        , {{ extract_json_field('attributes', ['conversationCounts', 'open']) }} as conversation_count_open 
        , {{ extract_json_field('attributes', ['conversationCounts', 'done']) }} as conversation_count_done
        , {{ extract_json_field('attributes', ['conversationCounts', 'snoozed']) }} as conversation_count_snoozed
        , {{ extract_json_field('attributes', ['conversationCounts', 'all']) }} as conversation_count_all
        , {{ extract_json_field('attributes', ['lastConversation', 'channels']) }} as last_conversation_channels
        , {{ extract_json_field('attributes', ['lastConversation', 'tags']) }} as last_conversation_tags
        , {{ extract_json_field('attributes', ['lastMessageOut', 'sentAt']) }} as last_message_sent_at
        , {{ extract_json_field('attributes', ['lastActivityAt']) }} as last_activity_at
        , {{ extract_json_field('attributes', ['lastMessageAt']) }} as last_message_at
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by -- TODO always null
        , updated_at

    from source

)

select * from final