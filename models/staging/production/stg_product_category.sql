with 
    source_data as (
        select *
        from {{ source('source_production', 'production_productcategory') }}
    )
    
    , renamed as (
        select
            cast(productcategoryid as integer) as product_category_id
            , cast(name as varchar) as product_category_name
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed