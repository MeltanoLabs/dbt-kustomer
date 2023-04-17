with source as (

    select * from {{ source('kustomer', 'conversations') }}

)

, final as (

    select
        id as conversation_id
        , {{ extract_json_field('attributes', ['deletedBy']) }} as deleted_by
        , {{ extract_json_field('attributes', ['createdBy']) }} as created_by
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by
        , {{ extract_json_field('relationships', ['queue', 'data', 'id']) }} as queue_id
        , {{ extract_json_field('relationships', ['slaVersion', 'data', 'id']) }} as sla_version_id
        , null as deleted -- TODO populate this field
        , {{ extract_json_field('attributes', ['direction']) }} as direction 
        , {{ extract_json_field('attributes', ['externalId']) }} as external_id 
        , {{ extract_json_field('attributes', ['messageCount']) }} as message_count 
        , {{ extract_json_field('attributes', ['name']) }} as name 
        , {{ extract_json_field('attributes', ['noteCount']) }} as note_count 
        , {{ extract_json_field('attributes', ['outboundMessageCount']) }} as outbound_message_count 
        , {{ extract_json_field('attributes', ['reopenCount']) }} as reopen_count 
        , null as replay_channel -- TODO populate this field
        , {{ extract_json_field('attributes', ['rev']) }} as rev 
        , {{ extract_json_field('attributes', ['satisfaction']) }} as satisfaction 
        , null as sentiment_polarity -- TODO populate this field
        , null as sentiment_confidence -- TODO populate this field
        , {{ extract_json_field('attributes', ['snoozeCount']) }} as snooze_count 
        , {{ extract_json_field('attributes', ['status']) }} as status 
        , {{ extract_json_field('attributes', ['firstMessageIn', 'channel']) }} as first_message_in_channel 
        , {{ extract_json_field('attributes', ['firstMessageIn', 'createdAt']) }} as first_message_in_created_at -- TODO Check field name
        , {{ extract_json_field('attributes', ['firstMessageIn', 'id']) }} as first_message_in_id
        , {{ extract_json_field('attributes', ['firstMessageIn', 'meta', 'recipient', 'email']) }} as first_message_in_meta_recipient_email
        , {{ extract_json_field('attributes', ['firstMessageIn', 'meta', 'recipient', 'mailboxHash']) }} as first_message_in_meta_recipient_mailboxhash -- TODO Check field name
        , {{ extract_json_field('attributes', ['firstMessageIn', 'meta', 'to']) }} as first_message_in_meta_to
        , {{ extract_json_field('attributes', ['firstMessageIn', 'sentAt', 'to']) }} as first_message_in_sent_at -- TODO Check field name
        , {{ extract_json_field('attributes', ['firstResponse', 'createdAt']) }} as first_response_created_at
        , {{ extract_json_field('attributes', ['firstResponse', 'id']) }} as first_response_id
        , {{ extract_json_field('attributes', ['firstResponse', 'responseTime']) }} as first_response_response_time
        , {{ extract_json_field('attributes', ['firstResponse', 'sentAt']) }} as first_response_sent_at
        , {{ extract_json_field('attributes', ['firstResponse', 'time']) }} as first_response_time
        -- first_company_* -- TODO check field names required and populate
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'formResponse']) }} as satisfaction_level_form_response
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'form']) }} as satisfaction_level_form
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'channel']) }} as satisfaction_level_channel
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'status']) }} as satisfaction_level_status
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'scheduledFor']) }} as satisfaction_level_scheduled_for
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'createdAt']) }} as satisfaction_level_created_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentAt']) }} as satisfaction_level_sent_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'updatedAt']) }} as satisfaction_level_updated_at
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'answers']) }} as satisfaction_level_answers
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentBy']) }} as satisfaction_level_sent_by
        , {{ extract_json_field('attributes', ['satisfactionLevel', 'sentByTeams']) }} as satisfaction_level_sent_by_teams
        , {{ extract_json_field('attributes', ['lastMessageIn', 'id']) }} as last_message_in_id
        , {{ extract_json_field('attributes', ['lastMessageIn', 'sentAt']) }} as last_message_in_sent_at
        , {{ extract_json_field('attributes', ['lastMessageIn', 'createdAt']) }} as last_message_in_created_at
        , {{ extract_json_field('attributes', ['lastMessageIn', 'meta', 'to']) }} as last_message_in_meta_to
        , {{ extract_json_field('attributes', ['lastMessageIn', 'meta', 'from']) }} as last_message_in_meta_from
        , {{ extract_json_field('attributes', ['lastMessageIn', 'channel']) }} as last_message_in_channel
        , {{ extract_json_field('attributes', ['lastMessageOut', 'id']) }} as last_message_out_id
        , {{ extract_json_field('attributes', ['lastMessageOut', 'sentAt']) }} as last_message_out_sent_at
        , {{ extract_json_field('attributes', ['lastMessageOut', 'createdAt']) }} as last_message_out_created_at
        , {{ extract_json_field('attributes', ['lastMessageOut', 'createdBy']) }} as last_message_out_created_by
        -- sentiment_* -- TODO check field names required and populate
        , {{ extract_json_field('attributes', ['sla', 'name']) }} as sla_name
        , {{ extract_json_field('attributes', ['sla', 'matchedAt']) }} as sla_matched_at
        , {{ extract_json_field('attributes', ['sla', 'metrics']) }} as sla_metrics
        , {{ extract_json_field('attributes', ['sla', 'breached']) }} as sla_breached
        , {{ extract_json_field('attributes', ['sla', 'status']) }} as sla_status
        , {{ extract_json_field('attributes', ['sla', 'summary']) }} as sla_summary
        , {{ extract_json_field('attributes', ['sla', 'version']) }} as sla_version
        -- duplicate here in ERD of sla_version
        -- duplicate here in ERD of sla_version_id
        /* Meltano specific field */
        , updated_at

    from source

)

select * from final