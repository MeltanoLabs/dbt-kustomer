with source as (

    select * from {{ source('kustomer', 'customers') }}

)

, final as (

    select
        id as customer_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['displayName']) }} as display_name
        , {{ extract_json_field('attributes', ['displayColor']) }} as display_color
        , {{ extract_json_field('attributes', ['displayIcon']) }} as display_icon
        , {{ extract_json_field('attributes', ['externalIds']) }} as external_ids
        , {{ extract_json_field('attributes', ['sharedExternalIds']) }} as shared_external_ids
        , {{ extract_json_field('attributes', ['emails']) }} as emails -- JSON array
        , {{ extract_json_field('attributes', ['sharedEmails']) }} as shared_emails -- JSON array
        , {{ extract_json_field('attributes', ['phones']) }} as phones -- JSON array
        , {{ extract_json_field('attributes', ['sharedPhones']) }} as shared_phones
        , {{ extract_json_field('attributes', ['whatsapps']) }} as whatsapps
        , {{ extract_json_field('attributes', ['sharedWhatsapps']) }} as shared_whatsapps
        , {{ extract_json_field('attributes', ['facebookIds']) }} as facebook_ids
        , {{ extract_json_field('attributes', ['instagramIds']) }} as instagram_ids
        , {{ extract_json_field('attributes', ['socials']) }} as socials
        , {{ extract_json_field('attributes', ['sharedSocials']) }} as shared_socials
        , {{ extract_json_field('attributes', ['urls']) }} as urls
        , {{ extract_json_field('attributes', ['locations']) }} as locations -- JSON array
        , {{ extract_json_field('attributes', ['activeUsers']) }} as active_users
        , {{ extract_json_field('attributes', ['watchers']) }} as watchers
        , {{ extract_json_field('attributes', ['recentLocation', 'location']) }} as recent_location
        , {{ extract_json_field('attributes', ['recentLocation', 'updatedAt']) }} as recent_location_updated_at
        , {{ extract_json_field('attributes', ['locale']) }} as locale
        , {{ extract_json_field('attributes', ['timeZone']) }} as time_zone
        , {{ extract_json_field('attributes', ['birthdayAt']) }} as birthday_at
        , {{ extract_json_field('attributes', ['gender']) }} as gender
        , {{ extract_json_field('attributes', ['presence']) }} as presence
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['lastSeenAt']) }} as last_seen_at
        , {{ extract_json_field('attributes', ['lastActivityAt']) }} as last_activity_at
        , {{ extract_json_field('attributes', ['lastCustomerActivityAt']) }} as last_customer_activity_at
        , {{ extract_json_field('attributes', ['deleted']) }} as deleted
        , {{ extract_json_field('attributes', ['lastMessageIn', 'channel']) }} as last_message_in_channel
        , {{ extract_json_field('attributes', ['lastMessageIn', 'sentiment']) }} as last_message_in_sentiment
        , {{ extract_json_field('attributes', ['lastMessageIn', 'sentAt']) }} as last_message_in_sent_at
        , {{ extract_json_field('attributes', ['lastMessageOut', 'sentAt']) }} as last_message_out_sent_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedTo', 'sentAt']) }} as last_message_unresponded_to_sent_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedTo', 'channel']) }} as last_message_unresponded_to_channel
        , {{ extract_json_field('attributes', ['lastMessageAt']) }} as last_message_at
        , {{ extract_json_field('attributes', ['lastConversation', 'id']) }} as last_conversation_id
        , {{ extract_json_field('attributes', ['lastConversation', 'channels']) }} as last_conversation_channels
        , {{ extract_json_field('attributes', ['lastConversation', 'tags']) }} as last_conversation_tags
        , {{ extract_json_field('attributes', ['lastConversation', 'sentiment']) }} as last_conversation_sentiment
        , {{ extract_json_field('attributes', ['conversationCounts', 'open']) }} as conversation_counts_open
        , {{ extract_json_field('attributes', ['conversationCounts', 'done']) }} as conversation_counts_done
        , {{ extract_json_field('attributes', ['conversationCounts', 'snoozed']) }} as conversation_counts_snoozed
        , {{ extract_json_field('attributes', ['conversationCounts', 'all']) }} as conversation_counts_all
        , {{ extract_json_field('attributes', ['preview', 'channel']) }} as preview_channel
        , {{ extract_json_field('attributes', ['preview', 'subject']) }} as preview_subject
        , {{ extract_json_field('attributes', ['preview', 'text']) }} as preview_text
        , {{ extract_json_field('attributes', ['preview', 'type']) }} as preview_type
        , {{ extract_json_field('attributes', ['preview', 'previewAt']) }} as preview_preview_at
        , {{ extract_json_field('attributes', ['tags']) }} as tags
        , {{ extract_json_field('attributes', ['sentiment', 'polarity']) }} as sentiment_polarity
        , {{ extract_json_field('attributes', ['sentiment', 'confidence']) }} as sentiment_confidence
        , {{ extract_json_field('attributes', ['custom']) }} as custom -- JSON but structure varies, so can't expand it
        , {{ extract_json_field('attributes', ['progressiveStatus']) }} as progressive_status
        , {{ extract_json_field('attributes', ['verified']) }} as verified
        , {{ extract_json_field('attributes', ['rev']) }} as rev
        , {{ extract_json_field('attributes', ['recentItems']) }} as recent_items -- JSON array
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'count']) }} as satisfaction_level_count
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'conversation']) }} as first_satisfaction_conversation_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'sentByTeams']) }} as first_satisfaction_sent_by_teams
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'formResponse']) }} as first_satisfaction_form_response_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'form']) }} as first_satisfaction_form_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'channel']) }} as first_satisfaction_channel
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'status']) }} as first_satisfaction_status
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'scheduledFor']) }} as first_satisfaction_scheduled_for
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'createdAt']) }} as first_satisfaction_created_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'sentAt']) }} as first_satisfaction_sent_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'updatedAt']) }} as first_satisfaction_updated_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'firstSatisfaction', 'sentBy']) }} as first_satisfaction_sent_by
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'conversation']) }} as last_satisfaction_conversation_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'sentByTeams']) }} as last_satisfaction_sent_by_teams
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'formResponse']) }} as last_satisfaction_form_response_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'form']) }} as last_satisfaction_form_id
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'channel']) }} as last_satisfaction_channel
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'status']) }} as last_satisfaction_status
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'scheduledFor']) }} as last_satisfaction_scheduled_for
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'createdAt']) }} as last_satisfaction_created_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'sentAt']) }} as last_satisfaction_sent_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'updatedAt']) }} as last_satisfaction_updated_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'lastSatisfaction', 'sentBy']) }} as last_satisfaction_sent_by
        , {{ extract_json_field('attributes', ['roleGroupVersions']) }} as role_group_versions
        , {{ extract_json_field('attributes', ['accessOverride']) }} as access_override
        , {{ extract_json_field('attributes', ['firstName']) }} as first_name
        , {{ extract_json_field('attributes', ['lastName']) }} as last_name
        
        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
          
        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final
