with source as (

    select * from {{ source('kustomer', 'kobjects') }}

)

, final as (

    select
        id as kobject_id
        , type

        /* Attributes */
        , {{ extract_json_field('attributes', ['externalId']) }} as external_id
        , {{ extract_json_field('attributes', ['title']) }} as title
        , {{ extract_json_field('attributes', ['description']) }} as description
        , {{ extract_json_field('attributes', ['icon']) }} as icon
        , {{ extract_json_field('attributes', ['images']) }} as images
        , {{ extract_json_field('attributes', ['custom']) }} as custom -- JSON but structure varies, so can't expand it
        , {{ extract_json_field('attributes', ['tags']) }} as tags
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['roleGroupVersions']) }} as role_group_versions

        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['klass', 'data', 'id']) }} as klass_id
        , {{ extract_json_field('relationships', ['klass', 'data', 'type']) }} as klass_type
        , {{ extract_json_field('relationships', ['customer', 'data', 'id']) }} as customer_id

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final