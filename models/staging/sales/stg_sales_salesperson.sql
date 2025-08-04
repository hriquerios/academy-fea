with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesperson') }}
    )
    
    , renamed as (
        select
            cast(businessentityid as integer) as business_entity_id
            , cast(territoryid as integer) as territory_id
            , cast(salesquota as numeric(19,4)) as sales_quota
            , cast(bonus as numeric(19,4)) as bonus
            , cast(commissionpct as numeric(10,4)) as commission_pct
            , cast(salesytd as numeric(19,4)) as sales_ytd
            , cast(saleslastyear as numeric(19,4)) as sales_last_year
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed