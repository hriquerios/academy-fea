with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_customer') }}
    )
    
    , renamed as (
        select
            cast(customerid as integer) as customer_id
            , cast(personid as integer) as person_id
            , cast(storeid as integer) as store_id
            , cast(territoryid as integer) as territory_id
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed