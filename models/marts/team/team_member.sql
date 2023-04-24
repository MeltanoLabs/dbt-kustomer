with source as (

    select
        team_id
        , members

    from {{ ref('stg_kustomer__teams') }}

)

, unnest_array as (

    {{ flatten_json_array('source', 'team_id', 'members') }}
        
)

, final as (

    select
        team_id
        , members as user_id

    from unnest_array

)

select * from final