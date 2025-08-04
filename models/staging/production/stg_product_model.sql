with 
    source_data as (
        select *
        from {{ source('source_production', 'production_productmodel') }}
    )
    
    , renamed as (
        select
            cast(productmodelid as integer) as product_model_id
            , cast(name as varchar) as product_model_name
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed