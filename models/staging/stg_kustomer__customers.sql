with source as (

    select * from {{ source('kustomer', 'customers') }}

)

, final as (

    select
        id as customer_id
        , attributes:externalId as external_id
        , attributes:name as name
        , attributes:firstName as first_name
        , attributes:lastName as last_name
        , attributes:displayName as display_name
        , attributes:gender as gender
        , attributes:defaultLang as default_language
        , attributes:locale as locale
        , attributes:displayColor as display_color
        , attributes:progressiveStatus as progressive_status
        , attributes:deleted as is_deleted
        , attributes:verified as is_verified
        , attributes:conversationCounts:open as conversation_count_open 
        , attributes:conversationCounts:done as conversation_count_done
        , attributes:conversationCounts:snoozed as conversation_count_snoozed
        , attributes:conversationCounts:all as conversation_count_all
        , attributes:lastConversation:channels as last_conversation_channels
        , attributes:lastConversation:tags as last_conversation_tags
        , attributes:lastMessageOut:sentAt as last_message_sent_at
        , attributes:lastActivityAt as last_activity_at
        , attributes:lastMessageAt as last_message_at
        , relationships:modifiedBy:data:id as modified_by
        , relationships:createdBy:data:id as created_by -- TODO always null
        , updated_at

    from source

)

select * from final