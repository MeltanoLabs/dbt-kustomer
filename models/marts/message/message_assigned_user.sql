with source as (

    select 
        message_id
        , assigned_users
    from {{ ref('stg_kustomer__messages') }}

)

, unnest_array as (

    {{ flatten_json_array('source', 'message_id', 'assigned_users') }}
        
)

, final as (

    select
        message_id
        , assigned_users as user_id

    from unnest_array

)

select * from final