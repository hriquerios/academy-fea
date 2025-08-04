with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesorderheadersalesreason') }}
    )
    
    , renamed as (
        select
            cast(salesorderid as integer) as sales_order_id
            , cast(salesreasonid as integer) as sales_reason_id
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed