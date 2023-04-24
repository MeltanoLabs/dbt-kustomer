with source as (

    select * from {{ source('kustomer', 'attachments') }}

)

, final as (

    select
        id as attachment_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['contentType']) }} as content_type
        , {{ extract_json_field('attributes', ['contentLength']) }} as content_length
        , {{ extract_json_field('attributes', ['redacted']) }} as redacted        
        
        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['message', 'data', 'id']) }} as message_id

        /* Links */
        , {{ extract_json_field('links', ['self']) }} as link_self
        , {{ extract_json_field('links', ['related']) }} as link_related
    
    from source

)

select * from final
