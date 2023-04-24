with source as (

    select * from {{ source('kustomer', 'tags') }}

)

, final as (

    select
        id as tag_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['deleted']) }} as deleted
        , {{ extract_json_field('attributes', ['deletedAt']) }} as deleted_at
        , {{ extract_json_field('attributes', ['color']) }} as color
            
        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id
        , {{ extract_json_field('relationships', ['deletedBy', 'data', 'id']) }} as deleted_by_id

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final