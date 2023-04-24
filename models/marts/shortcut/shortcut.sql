with source as (

    select * from {{ ref('stg_kustomer__shortcuts') }}

)  
    
, final as (

    select
        shortcut_id as id
        , created_by_id as created_by
        , modified_by_id as modified_by
        , deleted as conversation_deleted -- TODO check this is the correct field
        , null as conversation_direction -- TODO work out where this field is from
        , null as conversation_first_response -- TODO work out where this field is from
        , null as conversation_last_message_in -- TODO work out where this field is from
        , null as conversation_message_count -- TODO work out where this field is from
        , null as conversation_name -- TODO work out where this field is from
        , null as conversation_note_count -- TODO work out where this field is from
        , null as conversation_outbound_message_count -- TODO work out where this field is from
        , null as conversation_satisfaction -- TODO work out where this field is from
        , null as conversation_sentiment_confidence -- TODO work out where this field is from
        , null as conversation_sentiment_polarity -- TODO work out where this field is from
        , null as conversation_status -- TODO work out where this field is from
        , rev as conversation_rev -- TODO check this is the correct field
        , draft_text as draft -- TODO check this is the correct field
        , name
        , payload

    from source

)

select * from final
