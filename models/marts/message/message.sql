with source as (

    select * from {{ ref('stg_kustomer__messages') }}

)

, final as (

    select
        message_id as id
        , conversation_id
        , created_by_id as created_by
        , customer_id
        , modified_by_id as modified_by
        , app
        , null as source -- TODO identify this field
        , channel
        , direction
        , preview
        , size
        , status -- There is also meta_status - what's the difference?
        , meta_subject as subject

    from source

)

select * from final

        