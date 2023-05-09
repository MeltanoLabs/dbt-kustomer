with source as (

    select * from {{ ref('stg_kustomer__attachments') }}

)

, final as (

    select
        attachment_id as id
        , message_id
        , null as created_by -- TODO identify this field
        , null as modified_by -- TODO identify this field
        , name
        , content_length
        , content_type
        , null as created_at -- TODO identify this field
        , redacted
        , link_self as self -- TODO check if this is the correct field
        , link_related as related -- TODO check if this is the correct field
        , null as updated_at -- TODO identify this field

    from source

)

select * from final

        