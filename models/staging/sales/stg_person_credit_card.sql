with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_personcreditcard') }}
    )
    
    , renamed as (
        select
            cast(businessentityid as integer) as business_entity_id
            , cast(creditcardid as integer) as credit_card_id
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed