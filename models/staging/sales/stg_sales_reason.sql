with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesreason') }}
    )
    
    , renamed as (
        select
            cast(salesreasonid as integer) as sales_reason_id
            , cast(name as varchar) as sales_reason_name
            , cast(reasontype as varchar) as sales_reason_type
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed