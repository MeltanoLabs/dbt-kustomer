with source as (

    select * from {{ source('kustomer', 'messages') }}

)

, final as (

    select
        id as message_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['channel']) }} as channel
        , {{ extract_json_field('attributes', ['app']) }} as app
        , {{ extract_json_field('attributes', ['size']) }} as size
        , {{ extract_json_field('attributes', ['direction']) }} as direction
        , {{ extract_json_field('attributes', ['directionType']) }} as direction_type
        , {{ extract_json_field('attributes', ['preview']) }} as preview
        , {{ extract_json_field('attributes', ['meta', 'trackingId']) }} as meta_tracking_id
        , {{ extract_json_field('attributes', ['meta', 'from']) }} as meta_from
        , {{ extract_json_field('attributes', ['meta', 'to']) }} as meta_to
        , {{ extract_json_field('attributes', ['meta', 'cc']) }} as meta_cc
        , {{ extract_json_field('attributes', ['meta', 'bcc']) }} as meta_bcc
        , {{ extract_json_field('attributes', ['meta', 'subject']) }} as meta_subject
        , {{ extract_json_field('attributes', ['meta', 'inReplyTo']) }} as meta_in_reply_to
        , {{ extract_json_field('attributes', ['meta', 'payload']) }} as meta_payload
        , {{ extract_json_field('attributes', ['meta', 'placedAt']) }} as meta_placed_at
        , {{ extract_json_field('attributes', ['meta', 'externalRecordingUrl']) }} as meta_external_recording_url
        , {{ extract_json_field('attributes', ['meta', 'status']) }} as meta_status
        , {{ extract_json_field('attributes', ['meta', 'answeredAt']) }} as meta_answered_at
        , {{ extract_json_field('attributes', ['meta', 'endedAt']) }} as meta_ended_at
        , {{ extract_json_field('attributes', ['meta', 'permalink']) }} as meta_permalink
        , {{ extract_json_field('attributes', ['meta', 'messageType']) }} as meta_mesage_type
        , {{ extract_json_field('attributes', ['status']) }} as status
        , {{ extract_json_field('attributes', ['assignedTeams']) }} as assigned_teams
        , {{ extract_json_field('attributes', ['assignedUsers']) }} as assigned_users
        , {{ extract_json_field('attributes', ['auto']) }} as auto
        , {{ extract_json_field('attributes', ['sentAt']) }} as sent_at
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['redacted']) }} as redacted
        , {{ extract_json_field('attributes', ['createdByTeams']) }} as created_by_teams
        , {{ extract_json_field('attributes', ['lang']) }} as lang
        , {{ extract_json_field('attributes', ['rev']) }} as rev
        , {{ extract_json_field('attributes', ['reactions']) }} as reactions
        , {{ extract_json_field('attributes', ['firstDelivered', 'timestamp']) }} as first_delivered_timestamp
        , {{ extract_json_field('attributes', ['firstDelivered', 'clientType']) }} as first_delivered_client_type
        , {{ extract_json_field('attributes', ['firstRead', 'timestamp']) }} as first_read_timestamp
        , {{ extract_json_field('attributes', ['intentDetections']) }} as intent_detections
        
        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id
        , {{ extract_json_field('relationships', ['conversation', 'data', 'id']) }} as conversation_id
        , {{ extract_json_field('relationships', ['shortcuts', 'data']) }} as shortcuts -- Array of objects

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final