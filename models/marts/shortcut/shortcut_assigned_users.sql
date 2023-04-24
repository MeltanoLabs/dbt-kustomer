with source as (

    select
        shortcut_id
        , access_users
    
    from {{ ref('stg_kustomer__shortcuts') }}

) 

, unnest_array as (

    {{ flatten_json_array('source', 'shortcut_id', 'access_users') }}
        
)
    
, final as (

    select
        , shortcut_id as shortcut_id
        , {{ extract_json_field('access_users', ['id']) }} as user_id
    from source

)

select * from final