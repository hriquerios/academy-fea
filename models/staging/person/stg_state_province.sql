with 
    source_data as (
        select
            *
        from {{ source('source_person', 'person_stateprovince') }}
    )
    
    , renamed as (
        select
            cast(stateprovinceid as integer) as state_province_id
            , cast(stateprovincecode as varchar) as state_province_code
            , cast(countryregioncode as varchar) as country_region_code
            , cast(name as varchar) as state_province_name
            , cast(territoryid as integer) as territory_id
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed