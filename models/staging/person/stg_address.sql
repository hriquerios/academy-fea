with
    source_data as (
        select *
        from {{ source('source_person', 'person_address') }}
    )
    
    , renamed as (
        select 
            cast(addressid as integer) as address_id
            , cast(addressline1 as varchar(60)) as address_line_1
            , cast(addressline2 as varchar(60)) as address_line_2
            , cast(city as varchar) as city_name
            , cast(stateprovinceid as integer) as state_province_id
            , cast(postalcode as varchar) as postal_code
            , cast(spatiallocation as varchar) as spatial_location
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )

select *
from renamed