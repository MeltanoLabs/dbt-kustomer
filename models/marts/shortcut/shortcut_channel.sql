{# TODO - Unsure where this data is from #}

with source as (

    select shortcut_id
    from {{ ref('stg_kustomer__shortcuts') }}

)  
    
, final as (

    select
        null as id -- TODO work out where this field is from
        , shortcut_id as shortcut_id
        , null as name -- TODO work out where this field is from
    from source

)

select * from final