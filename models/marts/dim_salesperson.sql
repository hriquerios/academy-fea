with
    salesperson as (
        select *
        from {{ ref('stg_sales_salesperson') }}
    )
    
    , person as (
        select *
        from {{ ref('stg_person') }}
    )
    
    , territory as (
        select *
        from {{ ref('stg_sales_territory') }}
    )
    
    , final as (
        select
            hash(salesperson.business_entity_id) as sk_salesperson
            , salesperson.business_entity_id as salesperson_id
            , person.full_name as salesperson_name
            , person.first_name as salesperson_first_name
            , person.last_name as salesperson_last_name
            , person.person_type
            , territory.territory_name
            , territory.territory_group
            , territory.country_region_code as territory_country
            , salesperson.sales_quota
            , salesperson.sales_ytd
            , salesperson.sales_last_year
            , salesperson.bonus
            , salesperson.commission_pct
            , case 
                when salesperson.sales_quota is null then 'No Quota'
                when salesperson.sales_ytd >= salesperson.sales_quota then 'Above Quota'
                when salesperson.sales_ytd >= (salesperson.sales_quota * 0.8) then 'Near Quota'
                else 'Below Quota'
            end as quota_performance
            , case
                when salesperson.sales_ytd > salesperson.sales_last_year then 'Growth'
                when salesperson.sales_ytd = salesperson.sales_last_year then 'Stable'
                else 'Decline'
            end as ytd_vs_last_year
            , round(
                case 
                    when salesperson.sales_quota > 0 
                    then (salesperson.sales_ytd / salesperson.sales_quota) * 100 
                    else null 
                end, 2
            ) as quota_achievement_pct
        from salesperson
        left join person
            on salesperson.business_entity_id = person.business_entity_id
        left join territory
            on salesperson.territory_id = territory.territory_id
    )

select *
from final