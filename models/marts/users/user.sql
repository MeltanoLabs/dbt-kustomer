with source as (

    select * from {{ ref('stg_kustomer__users') }}

)

, final as (

    select
        user_id as id
        , null as deleted -- TODO Need to work out where this field is coming from
        , null as deleted_at -- TODO Need to work out where this field is coming from
        , display_name
        , email
        , email_signature
        , mobile
        , name

    from source

)

select * from final
