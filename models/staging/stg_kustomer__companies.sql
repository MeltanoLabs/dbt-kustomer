with source as (

    select * from {{ source('kustomer', 'companies') }}

)

, final as (

    select
        id as company_id
        , type

        /* Attributes */
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['tags']) }} as tags
        , {{ extract_json_field('attributes', ['domains']) }} as domains
        , {{ extract_json_field('attributes', ['emails']) }} as emails
        , {{ extract_json_field('attributes', ['phones']) }} as phones
        , {{ extract_json_field('attributes', ['whatapps']) }} as whatapps
        , {{ extract_json_field('attributes', ['socials']) }} as socials
        , {{ extract_json_field('attributes', ['urls']) }} as urls
        , {{ extract_json_field('attributes', ['locations']) }} as locations
        , {{ extract_json_field('attributes', ['rev']) }} as rev
        , {{ extract_json_field('attributes', ['roleGroupVersions']) }} as role_group_versions
        
        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id

        /* Meltano specific field */
        , updated_at as record_updated_at
    
    from source

)

select * from final
