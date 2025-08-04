with
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesterritory') }}
    )
    
    , renamed as (
        select 
            cast(territoryid as integer) as territory_id
            , cast(name as varchar) as territory_name
            , cast(countryregioncode as varchar) as country_region_code
            , cast("group" as varchar) as territory_group
            , cast(salesytd as numeric(19,4)) as sales_ytd
            , cast(saleslastyear as numeric(19,4)) as sales_last_year
            , cast(costytd as numeric(19,4)) as cost_ytd
            , cast(costlastyear as numeric(19,4)) as cost_last_year
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed