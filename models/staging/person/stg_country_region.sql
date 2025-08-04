with 
    source_data as (
        select *
        from {{ source('source_person', 'person_countryregion') }}
    )
    
    , renamed as (
        select
            cast(countryregioncode as varchar(3)) as country_region_code
            , cast(name as varchar) as country_name
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed