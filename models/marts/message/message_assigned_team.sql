with source as (

    select 
        message_id
        , assigned_teams
    from {{ ref('stg_kustomer__messages') }}

)

, unnest_array as (

    {{ flatten_json_array('source', 'message_id', 'assigned_teams') }}
        
)

, final as (

    select
        message_id
        , assigned_teams as team_id

    from unnest_array

)

select * from final