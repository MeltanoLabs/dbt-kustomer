with source as (

    select 
        message_id
        , shortcuts
    from {{ ref('stg_kustomer__messages') }}

)

, unnest_array as (

    {{ flatten_json_array('source', 'message_id', 'shortcuts') }}
        
)

, final as (

    select
        message_id
        , {{ extract_json_field('shortcuts', ['id']) }} as shortcut_id

    from unnest_array

)

select * from final