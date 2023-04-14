with source as (

    select * from {{ source('kustomer', 'conversations') }}

)

, final as (

    select
        id as conversation_id
        , attributes:deletedBy as deleted_by
        , attributes:createdBy as created_by
        , relationships:customer:data:id as customer_id
        , relationships:modifiedBy:data:id as modified_by
        , relationships:queue:data:id as queue_id
        , relationships:slaVersion:data:id as sla_version_id
        , attributes:deleted as is_deleted -- TO DO not in stream
        , attributes:direction as direction 
        , attributes:externalId as external_id 
        , attributes:messageCount as message_count 
        , attributes:name as name 
        , attributes:noteCount as note_count 
        , attributes:outboundMessageCount as outbound_message_count 
        , attributes:reopenCount as reopen_count 
        , attributes:rev as rev 
        , attributes:satisfaction as satisfaction 
        , attributes:snoozeCount as snooze_count 
        , attributes:status as status 
        , attributes:firstMessageIn:channel as first_message_in_channel 
        , attributes:firstMessageIn:createdAt as first_message_in_created_at
        , attributes:firstMessageIn:id as first_message_in_id
        , attributes:firstMessageIn:meta:recipient:email as first_message_in_meta_recipient_email
        , attributes:firstMessageIn:meta:recipient:mailboxHash as first_message_in_meta_recipient_mailbox_hash
        , attributes:firstMessageIn:meta:to as first_message_in_meta_to
        , attributes:firstMessageIn:sentAt:to as first_message_in_sent_at
        , attributes:firstResponse:createdAt as first_response_created_at
        , attributes:firstResponse:id as first_response_id
        , attributes:firstResponse:responseTime as first_response_response_time
        , attributes:firstResponse:sentAt as first_response_sent_at
        , attributes:firstResponse:time as first_response_time
        , attributes:satisfactionLevel:formResponse as satisfaction_level_form_response
        , attributes:satisfactionLevel:form as satisfaction_level_form
        , attributes:satisfactionLevel:channel as satisfaction_level_channel
        , attributes:satisfactionLevel:status as satisfaction_level_status
        , attributes:satisfactionLevel:scheduledFor as satisfaction_level_scheduled_for
        , attributes:satisfactionLevel:createdAt as satisfaction_level_created_at
        , attributes:satisfactionLevel:sentAt as satisfaction_level_sent_at
        , attributes:satisfactionLevel:updatedAt as satisfaction_level_updated_at
        , attributes:satisfactionLevel:answers as satisfaction_level_answers
        , attributes:satisfactionLevel:sentBy as satisfaction_level_sent_by
        , attributes:satisfactionLevel:sentByTeams as satisfaction_level_sent_by_teams
        , attributes:lastMessageIn:id as last_message_in_id
        , attributes:lastMessageIn:sentAt as last_message_in_sent_at
        , attributes:lastMessageIn:createdAt as last_message_in_created_at
        , attributes:lastMessageIn:meta:to as last_message_in_meta_to
        , attributes:lastMessageIn:meta:from as last_message_in_meta_from
        , attributes:lastMessageIn:channel as last_message_in_channel
        , attributes:lastMessageOut:id as last_message_out_id
        , attributes:lastMessageOut:sentAt as last_message_out_sent_at
        , attributes:lastMessageOut:createdAt as last_message_out_created_at
        , attributes:lastMessageOut:createdBy as last_message_out_created_by
        , attributes:sla:name as sla_name
        , attributes:sla:version as sla_version
        , attributes:sla:matchedAt as sla_matched_at
        , attributes:sla:metrics as sla_metrics
        , attributes:sla:breached as sla_breached
        , attributes:sla:status as sla_status
        , attributes:sla:summary as sla_summary
        , updated_at

    from source

)

select * from final