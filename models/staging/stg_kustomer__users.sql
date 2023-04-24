with source as (

    select * from {{ source('kustomer', 'shortcuts') }}

)

, final as (

    select
        id as user_id

        /* Attributes */
        , {{ extract_json_field('attributes', ['displayName']) }} as display_name
        , {{ extract_json_field('attributes', ['name']) }} as name
        , {{ extract_json_field('attributes', ['userType']) }} as user_type
        , {{ extract_json_field('attributes', ['email']) }} as email
        , {{ extract_json_field('attributes', ['emailVerifiedAt']) }} as email_verified_at
        , {{ extract_json_field('attributes', ['firstEmailVerifiedAt']) }} as first_email_verified_at
        , {{ extract_json_field('attributes', ['mobile']) }} as mobile
        , {{ extract_json_field('attributes', ['emailSignature']) }} as email_signature
        , {{ extract_json_field('attributes', ['password', 'allowNew']) }} as password_allow_new
        , {{ extract_json_field('attributes', ['password', 'forceNew']) }} as password_force_new
        , {{ extract_json_field('attributes', ['password', 'updatedAt']) }} as password_updated_at
        , {{ extract_json_field('attributes', ['verifiedEmailStatus']) }} as verified_email_status
        , {{ extract_json_field('attributes', ['roleGroups']) }} as role_groups
        , {{ extract_json_field('attributes', ['roles']) }} as roles
        , {{ extract_json_field('attributes', ['firstLoginAt']) }} as first_login_at
        , {{ extract_json_field('attributes', ['mostRecentLoginAt']) }} as most_recent_login_at
        , {{ extract_json_field('attributes', ['updatedAt']) }} as updated_at
        , {{ extract_json_field('attributes', ['createdAt']) }} as created_at
        , {{ extract_json_field('attributes', ['modifiedAt']) }} as modified_at
        , {{ extract_json_field('attributes', ['isEmailValid']) }} as is_email_valid

        /* Relationships */
        , {{ extract_json_field('relationships', ['org', 'data', 'id']) }} as organisation_id
        , {{ extract_json_field('relationships', ['createdBy', 'data', 'id']) }} as created_by_id
        , {{ extract_json_field('relationships', ['modifiedBy', 'data', 'id']) }} as modified_by_id

        /* Meltano specific field */
        , updated_at as record_updated_at

    from source

)

select * from final