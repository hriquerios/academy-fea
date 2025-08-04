with 
    source_data as (
        select *
        from {{ source('source_person', 'person_person') }}
    )
    
    , renamed as (
        select
            cast(businessentityid as integer) as business_entity_id
            , cast(persontype as varchar) as person_type
            , cast(firstname as varchar) as first_name
            , cast(middlename as varchar) as middle_name
            , cast(lastname as varchar) as last_name
            , trim(concat(
                firstname, 
                ' ', 
                coalesce(middlename || ' ', ''), 
                lastname
            )) as full_name
            , cast(emailpromotion as integer) as email_promotion
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed