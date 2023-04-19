with source as (

    select * from {{ source('kustomer', 'conversations') }}

)

, final as (

    select
        id as conversation_id
        
        /* Attributes */
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['preview']) }} as preview
        , {{ extract_json_field('attributes', ['channels']) }} as channels
        , {{ extract_json_field('attributes', ['status']) }} as status
        , {{ extract_json_field('attributes', ['open', 'statusAt']) }} as open_status_at
        , {{ extract_json_field('attributes', ['messageCount']) }} as message_count
        , {{ extract_json_field('attributes', ['lastReceivedAt']) }} as last_received_at
        , {{ extract_json_field('attributes', ['noteCount']) }} as note_count
        , {{ extract_json_field('attributes', ['satisfaction']) }} as satisfaction
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentByTeams']) }} as satisfaction_sent_by_teams
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'answers']) }} as satisfaction_answers
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'formResponse']) }} as satisfaction_form_response
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'form']) }} as satisfaction_form
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'channel']) }} as satisfaction_channel
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'status']) }} as satisfaction_status
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'scheduledFor']) }} as satisfaction_scheduled_for
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'createdAt']) }} as satisfaction_created_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentAt']) }} as satisfaction_sent_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'updatedAt']) }} as satisfaction_updated_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentBy']) }} as satisfaction_sent_by
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'rating']) }} as satisfaction_rating
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'score']) }} as satisfaction_score
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['lastActivityAt']) }} as last_activity_at
        , {{ extract_json_field('attributes', ['spam']) }} as spam
        , {{ extract_json_field('attributes', ['ended']) }} as ended
        , {{ extract_json_field('attributes', ['endedAt']) }} as ended_at
        , {{ extract_json_field('attributes', ['endedReason']) }} as ended_reason
        , {{ extract_json_field('attributes', ['endedByType']) }} as ended_by_type
        , {{ extract_json_field('attributes', ['tags']) }} as tags
        , {{ extract_json_field('attributes', ['suggestedTags']) }} as suggested_tags
        , {{ extract_json_field('attributes', ['predictions']) }} as predictions
        , {{ extract_json_field('attributes', ['suggestedShortcuts']) }} as suggested_shortcuts
        , {{ extract_json_field('attributes', ['firstMessageIn', 'firstDelivered', 'timestamp']) }} as first_message_in_first_delivered_timestamp
        , {{ extract_json_field('attributes', ['firstMessageIn', 'firstDelivered', 'clientType']) }} as first_message_in_first_delivered_client_type
        , {{ extract_json_field('attributes', ['firstMessageIn', 'id']) }} as first_message_in_id
        , {{ extract_json_field('attributes', ['firstMessageIn', 'sentAt']) }} as first_message_in_sent_at
        , {{ extract_json_field('attributes', ['firstMessageIn', 'createdAt']) }} as first_message_in_created_at
        , {{ extract_json_field('attributes', ['firstMessageIn', 'directionType']) }} as first_message_in_direction_type
        , {{ extract_json_field('attributes', ['firstMessageIn', 'channel']) }} as first_message_in_channel
        , {{ extract_json_field('attributes', ['firstMessageIn', 'meta']) }} as first_message_in_meta
        , {{ extract_json_field('attributes', ['firstMessageOut', 'createdByTeams']) }} as first_message_out_created_by_teams
        , {{ extract_json_field('attributes', ['firstMessageOut', 'id']) }} as first_message_out_id
        , {{ extract_json_field('attributes', ['firstMessageOut', 'sentAt']) }} as first_message_out_sent_at
        , {{ extract_json_field('attributes', ['firstMessageOut', 'createdAt']) }} as first_message_out_created_at
        , {{ extract_json_field('attributes', ['firstMessageOut', 'channel']) }} as first_message_out_channel
        , {{ extract_json_field('attributes', ['firstMessageOut', 'directionType']) }} as first_message_out_direction_type
        , {{ extract_json_field('attributes', ['firstMessageOut', 'createdBy']) }} as first_message_out_created_by
        , {{ extract_json_field('attributes', ['lastMessageIn', 'id']) }} as last_message_in_id
        , {{ extract_json_field('attributes', ['lastMessageIn', 'sentAt']) }} as last_message_in_sent_at
        , {{ extract_json_field('attributes', ['lastMessageIn', 'createdAt']) }} as last_message_in_created_at
        , {{ extract_json_field('attributes', ['lastMessageIn', 'meta']) }} as last_message_in_meta
        , {{ extract_json_field('attributes', ['lastMessageOut', 'id']) }} as last_message_out_id
        , {{ extract_json_field('attributes', ['lastMessageOut', 'sentAt']) }} as last_message_out_sent_at
        , {{ extract_json_field('attributes', ['lastMessageOut', 'createdAt']) }} as last_message_out_created_at
        , {{ extract_json_field('attributes', ['lastMessageOut', 'createdBy']) }} as last_message_out_created_by
        , {{ extract_json_field('attributes', ['lastMessageAt']) }} as last_message_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedTo', 'id']) }} as last_message_unresponded_to_id
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedTo', 'sentAt']) }} as last_message_unresponded_to_sent_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedTo', 'createdAt']) }} as last_message_unresponded_to_created_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedToSinceLastDone', 'id']) }} as last_message_unresponded_to_since_last_done_id
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedToSinceLastDone', 'sentAt']) }} as last_message_unresponded_to_since_last_done_sent_at
        , {{ extract_json_field('attributes', ['lastMessageUnrespondedToSinceLastDone', 'createdAt']) }} as last_message_unresponded_to_since_last_done_created_at
        , {{ extract_json_field('attributes', ['assignedUsers']) }} as assigned_users
        , {{ extract_json_field('attributes', ['assignedTeams']) }} as assigned_teams
        , {{ extract_json_field('attributes', ['firstResponse', 'createdByTeams']) }} as first_response_created_by_teams
        , {{ extract_json_field('attributes', ['firstResponse', 'assignedTeams']) }} as first_response_assigned_teams
        , {{ extract_json_field('attributes', ['firstResponse', 'assignedUsers']) }} as first_response_assigned_users
        , {{ extract_json_field('attributes', ['firstResponse', 'id']) }} as first_response_id
        , {{ extract_json_field('attributes', ['firstResponse', 'time']) }} as first_response_time
        , {{ extract_json_field('attributes', ['firstResponse', 'businessTime']) }} as first_response_business_time
        , {{ extract_json_field('attributes', ['firstResponse', 'createdAt']) }} as first_response_created_at
        , {{ extract_json_field('attributes', ['firstResponse', 'createdBy']) }} as first_response_created_by
        , {{ extract_json_field('attributes', ['firstResponse', 'sentAt']) }} as first_response_sent_at
        , {{ extract_json_field('attributes', ['firstResponse', 'responseTime']) }} as first_response_response_time
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'createdByTeams']) }} as first_response_since_last_done_created_by_teams
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'assignedTeams']) }} as first_response_since_last_done_assigned_teams
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'assignedUsers']) }} as first_response_since_last_done_assigned_users
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'id']) }} as first_response_since_last_done_id
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'time']) }} as first_response_since_last_done_time
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'businessTime']) }} as first_response_since_last_done_business_time
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'createdAt']) }} as first_response_since_last_done_created_at
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'createdBy']) }} as first_response_since_last_done_created_by
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'sentAt']) }} as first_response_since_last_done_sent_at
        , {{ extract_json_field('attributes', ['firstResponseSinceLastDone', 'responseTime']) }} as first_response_since_last_done_response_time
        , {{ extract_json_field('attributes', ['lastResponse', 'createdByTeams']) }} as last_response_created_by_teams  
        , {{ extract_json_field('attributes', ['lastResponse', 'assignedTeams']) }} as last_response_assigned_teams
        , {{ extract_json_field('attributes', ['lastResponse', 'assignedUsers']) }} as last_response_assigned_users
        , {{ extract_json_field('attributes', ['lastResponse', 'id']) }} as last_response_id
        , {{ extract_json_field('attributes', ['lastResponse', 'time']) }} as last_response_time
        , {{ extract_json_field('attributes', ['lastResponse', 'businessTime']) }} as last_response_business_time
        , {{ extract_json_field('attributes', ['lastResponse', 'createdAt']) }} as last_response_created_at
        , {{ extract_json_field('attributes', ['lastResponse', 'createdBy']) }} as last_response_created_by
        , {{ extract_json_field('attributes', ['lastResponse', 'sentAt']) }} as last_response_sent_at
        , {{ extract_json_field('attributes', ['lastResponse', 'responseTime']) }} as last_response_response_time
        , {{ extract_json_field('attributes', ['firstDone', 'createdByTeams']) }} as first_done_created_by_teams
        , {{ extract_json_field('attributes', ['firstDone', 'assignedTeams']) }} as first_done_assigned_teams
        , {{ extract_json_field('attributes', ['firstDone', 'assignedUsers']) }} as first_done_assigned_users
        , {{ extract_json_field('attributes', ['firstDone', 'id']) }} as first_done_id
        , {{ extract_json_field('attributes', ['firstDone', 'time']) }} as first_done_time
        , {{ extract_json_field('attributes', ['firstDone', 'businessTime']) }} as first_done_business_time
        , {{ extract_json_field('attributes', ['firstDone', 'createdAt']) }} as first_done_created_at
        , {{ extract_json_field('attributes', ['firstDone', 'createdBy']) }} as first_done_created_by
        , {{ extract_json_field('attributes', ['firstDone', 'noteCount']) }} as first_done_note_count
        , {{ extract_json_field('attributes', ['firstDone', 'messageCount']) }} as first_done_message_count
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'chat']) }} as first_done_message_count_by_channel_chat
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'form']) }} as first_done_message_count_by_channel_form
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'email']) }} as first_done_message_count_by_channel_email
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'instagram']) }} as first_done_message_count_by_channel_instagram
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'sms']) }} as first_done_message_count_by_channel_sms
        , {{ extract_json_field('attributes', ['firstDone', 'messageCountByChannel', 'voice']) }} as first_done_message_count_by_channel_voice
        , {{ extract_json_field('attributes', ['firstDone', 'outboundMessageCount']) }} as first_done_outbound_message_count
        , {{ extract_json_field('attributes', ['firstDone', 'outboundMessageCountByChannel', 'chat']) }} as first_done_outbound_message_count_by_channel_chat
        , {{ extract_json_field('attributes', ['firstDone', 'outboundMessageCountByChannel', 'email']) }} as first_done_outbound_message_count_by_channel_email
        , {{ extract_json_field('attributes', ['firstDone', 'outboundMessageCountByChannel', 'instagram']) }} as first_done_outbound_message_count_by_channel_instagram
        , {{ extract_json_field('attributes', ['firstDone', 'outboundMessageCountByChannel', 'sms']) }} as first_done_outbound_message_count_by_channel_sms
        , {{ extract_json_field('attributes', ['firstDone', 'lastMessageDirection']) }} as first_done_last_message_direction
        , {{ extract_json_field('attributes', ['firstDone', 'lastMessageDirectionType']) }} as first_done_last_message_direction_type
        , {{ extract_json_field('attributes', ['lastDone', 'createdByTeams']) }} as last_done_created_by_teams
        , {{ extract_json_field('attributes', ['lastDone', 'assignedTeams']) }} as last_done_assigned_teams
        , {{ extract_json_field('attributes', ['lastDone', 'assignedUsers']) }} as last_done_assigned_users
        , {{ extract_json_field('attributes', ['lastDone', 'id']) }} as last_done_id
        , {{ extract_json_field('attributes', ['lastDone', 'time']) }} as last_done_time
        , {{ extract_json_field('attributes', ['lastDone', 'businessTime']) }} as last_done_business_time
        , {{ extract_json_field('attributes', ['lastDone', 'createdAt']) }} as last_done_created_at
        , {{ extract_json_field('attributes', ['lastDone', 'createdBy']) }} as last_done_created_by
        , {{ extract_json_field('attributes', ['lastDone', 'noteCount']) }} as last_done_note_count
        , {{ extract_json_field('attributes', ['lastDone', 'messageCount']) }} as last_done_message_count
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'chat']) }} as last_done_message_count_by_channel_chat
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'form']) }} as last_done_message_count_by_channel_form
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'email']) }} as last_done_message_count_by_channel_email
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'instagram']) }} as last_done_message_count_by_channel_instagram
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'sms']) }} as last_done_message_count_by_channel_sms
        , {{ extract_json_field('attributes', ['lastDone', 'messageCountByChannel', 'voice']) }} as last_done_message_count_by_channel_voice
        , {{ extract_json_field('attributes', ['lastDone', 'outboundMessageCount']) }} as last_done_outbound_message_count
        , {{ extract_json_field('attributes', ['lastDone', 'outboundMessageCountByChannel', 'chat']) }} as last_done_outbound_message_count_by_channel_chat
        , {{ extract_json_field('attributes', ['lastDone', 'outboundMessageCountByChannel', 'email']) }} as last_done_outbound_message_count_by_channel_email
        , {{ extract_json_field('attributes', ['lastDone', 'outboundMessageCountByChannel', 'instagram']) }} as last_done_outbound_message_count_by_channel_instagram
        , {{ extract_json_field('attributes', ['lastDone', 'outboundMessageCountByChannel', 'sms']) }} as last_done_outbound_message_count_by_channel_sms
        , {{ extract_json_field('attributes', ['lastDone', 'lastMessageDirection']) }} as last_done_last_message_direction
        , {{ extract_json_field('attributes', ['lastDone', 'lastMessageDirectionType']) }} as last_done_last_message_direction_type
        , {{ extract_json_field('attributes', ['doneCount']) }} as done_count
        , {{ extract_json_field('attributes', ['direction']) }} as direction
        , {{ extract_json_field('attributes', ['custom']) }} as custom -- JSON but structure varies, so can't expand it
        , {{ extract_json_field('attributes', ['lastMessageDirection']) }} as last_message_direction
        , {{ extract_json_field('attributes', ['outboundMessageCount']) }} as outbound_message_count
        , {{ extract_json_field('attributes', ['inboundMessageCount']) }} as inbound_message_count
        , {{ extract_json_field('attributes', ['rev']) }} as rev
        , {{ extract_json_field('attributes', ['priority']) }} as priority
        , {{ extract_json_field('attributes', ['defaultLang']) }} as default_lang
        , {{ extract_json_field('attributes', ['locale']) }} as locale 
        , {{ extract_json_field('attributes', ['totalOpen', 'businessTime'] )}} as total_open_business_time
        , {{ extract_json_field('attributes', ['totalOpen', 'businessTimeBySchedule'] )}} as total_open_business_time_by_schedule
        , {{ extract_json_field('attributes', ['totalOpen', 'businessTimeByScheduleSinceLastDone'] )}} as total_open_business_time_by_schedule_since_last_done
        , {{ extract_json_field('attributes', ['totalOpen', 'businessTimeSinceLastDone'] )}} as total_open_business_time_since_last_done
        , {{ extract_json_field('attributes', ['totalOpen', 'time'] )}} as total_open_time
        , {{ extract_json_field('attributes', ['totalOpen', 'timeSinceLastDone'] )}} as total_open_time_since_last_done
        , {{ extract_json_field('attributes', ['roleGroupVersions']) }} as role_group_versions
        , {{ extract_json_field('attributes', ['accessOverride']) }} as access_override
        , {{ extract_json_field('attributes', ['modificationHistory', 'nameAt']) }} as modification_history_name_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'priorityAt']) }} as modification_history_priority_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'channelAt']) }} as modification_history_channel_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'assignedTeamsAt']) }} as modification_history_assigned_teams_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'assignedUsersAt']) }} as modification_history_assigned_users_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'brandAt']) }} as modification_history_brand_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'defaultLangAt']) }} as modification_history_default_lang_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'statusAt']) }} as modification_history_status_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'tagsAt']) }} as modification_history_tags_at
        , {{ extract_json_field('attributes', ['modificationHistory', 'customAt']) }} as modification_history_custom_at
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'isFac']) }} as assistant_is_fac
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'source', 'type']) }} as assistant_fac_source_type
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'source', 'channel']) }} as assistant_fact_source_channel
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'createdAt']) }} as assistant_fac_created_at
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'reasons']) }} as assistant_fac_reasons
        , {{ extract_json_field('attributes', ['assistant', 'fac', 'exclusions']) }} as assistant_fac_exclusions
        , {{ extract_json_field('attributes', ['assistant', 'assistantId']) }} as assistant_id
        , {{ extract_json_field('attributes', ['assistant', 'status']) }} as assistant_status
        , {{ extract_json_field('attributes', ['assistant', 'type']) }} as assistant_type
        , {{ extract_json_field('attributes', ['assistant', 'emulation']) }} as assistant_emulation
        , {{ extract_json_field('attributes', ['assistant', 'transferredAt']) }} as assistant_transferred_at
        , {{ extract_json_field('attributes', ['assistant', 'app']) }} as assistant_app
        , {{ extract_json_field('attributes', ['assistant', 'channel']) }} as assistant_channel
        , {{ extract_json_field('attributes', ['phase']) }} as phase
        , {{ extract_json_field('attributes', ['matchedTimeBasedRules']) }} as matched_time_based_rules
        , {{ extract_json_field('attributes', ['externalId']) }} as external_id
        , {{ extract_json_field('attributes', ['replyChannel']) }} as reply_channel
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['totalDone', 'businessTime']) }} as total_done_business_time
        , {{ extract_json_field('attributes', ['totalDone', 'time']) }} as total_done_time
        , {{ extract_json_field('attributes', ['sla', 'name']) }} as sla_name
        , {{ extract_json_field('attributes', ['sla', 'version']) }} as sla_version
        , {{ extract_json_field('attributes', ['sla', 'matchedAt']) }} as sla_matched_at
        , {{ extract_json_field('attributes', ['sla', 'metrics', 'longestUnrespondedMessage', 'satisfiedAt']) }} as sla_metrics_longest_unresponded_message_satisfied_at
        , {{ extract_json_field('attributes', ['sla', 'metrics', 'longestUnrespondedMessage', 'breachAt']) }} as sla_metrics_longest_unresponded_message_breach_at
        , {{ extract_json_field('attributes', ['sla', 'breached']) }} as sla_breached
        , {{ extract_json_field('attributes', ['sla', 'status']) }} as sla_status
        , {{ extract_json_field('attributes', ['sla', 'summary', 'satisfiedAt']) }} as sla_summary_satisfied_at
        , {{ extract_json_field('attributes', ['sla', 'summary', 'firstBreachAt']) }} as sla_summary_first_breach_at
        , {{ extract_json_field('attributes', ['sla', 'breach', 'metric']) }} as sla_breach_metric
        , {{ extract_json_field('attributes', ['sla', 'breach', 'at']) }} as sla_breach_at
        , {{ extract_json_field('attributes', ['attribution', 'totalRevenue']) }} as attribution_total_revenue
        , {{ extract_json_field('attributes', ['attribution', 'hasAttributableRevenue']) }} as attribution_has_attributable_revenue
        , {{ extract_json_field('attributes', ['attribution', 'currency']) }} as attribution_currency
        , {{ extract_json_field('attributes', ['importedAt']) }} as imported_at
        , {{ extract_json_field('attributes', ['skills']) }} as skills
        , {{ extract_json_field('attributes', ['lastDeflection', 'deflectedAt']) }} as last_deflection_deflected_at
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'visited']) }} as last_deflection_articles_visited
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'deflected']) }} as last_deflection_articles_deflected
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'id']) }} as last_deflection_articles_id
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'version']) }} as last_deflection_articles_version
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'lang']) }} as last_deflection_articles_lang
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'title']) }} as last_deflection_articles_title
        , {{ extract_json_field('attributes', ['lastDeflection', 'articles', 'url']) }} as last_deflection_articles_url
        , {{ extract_json_field('attributes', ['lastDeflection', 'type']) }} as last_deflection_type
        , {{ extract_json_field('attributes', ['lastDeflection', 'status']) }} as last_deflection_status
        , {{ extract_json_field('attributes', ['lastDeflection', 'query', 'rawText']) }} as last_deflection_query_raw_text
        , {{ extract_json_field('attributes', ['snooze']) }} as snooze
        , {{ extract_json_field('attributes', ['snoozeCount']) }} as snooze_count
        , {{ extract_json_field('attributes', ['reopenCount']) }} as reopen_count
        , {{ extract_json_field('attributes', ['reopenFromDoneCount']) }} as reopen_from_done_count
        , {{ extract_json_field('attributes', ['totalSnooze']) }} as total_snooze
        , {{ extract_json_field('attributes', ['linkedConversations']) }} as linked_conversations
        , {{ extract_json_field('attributes', ['mergedTarget']) }} as merged_target
        , {{ extract_json_field('attributes', ['externalQueue']) }} as external_queue

        /* Relationship */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('relationships', ['brand', 'data', 'id']) }} as brand_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id
        , {{ extract_json_field('relationships', ['endedBy', 'data', 'id']) }} as ended_by_id
        , {{ extract_json_field('relationships', ['sla', 'data', 'id']) }} as sla_id
        , {{ extract_json_field('relationships', ['slaVersion', 'data', 'id']) }} as sla_version_id
        , {{ extract_json_field('relationships', ['queue', 'data', 'id']) }} as queue_id
        , {{ extract_json_field('relationships', ['subStatus', 'data', 'type']) }} as sub_status_id
         
        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final