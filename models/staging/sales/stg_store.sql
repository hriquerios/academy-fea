with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_store') }}
    )
    
    , renamed as (
        select
            cast(businessentityid as integer) as business_entity_id
            , cast(name as varchar) as store_name
            , cast(salespersonid as integer) as sales_person_id
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed