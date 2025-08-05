with
    address as (
        select *
        from {{ ref('stg_address') }}
    )
    
    , state_province as (
        select *
        from {{ ref('stg_state_province') }}
    )
    
    , country_region as (
        select *
        from {{ ref('stg_country_region') }}
    )
    
    , territory as (
        select *
        from {{ ref('stg_sales_territory') }}
    )
    
    , joined as (
        select
            address.address_id
            , address.address_line_1
            , address.city_name
            , address.postal_code
            , state_province.state_province_name
            , state_province.state_province_code
            , country_region.country_name
            , country_region.country_region_code
            , territory.territory_name
            , territory.territory_group
            , territory.territory_id
        from address
        left join state_province
            on address.state_province_id = state_province.state_province_id
        left join country_region
            on state_province.country_region_code = country_region.country_region_code
        left join territory
            on state_province.territory_id = territory.territory_id
    )
    
    , final as (
        select
            hash(address_id) as sk_location
            , address_id
            , address_line_1
            , city_name
            , postal_code
            , state_province_name
            , state_province_code
            , country_name
            , country_region_code
            , territory_name
            , territory_group
            , territory_id
            , concat(city_name, ', ', state_province_name) as city_state
            , concat(city_name, ', ', state_province_name, ', ', country_name) as full_address
        from joined
    )

select *
from final