with
    customer as (
        select *
        from {{ ref('stg_customer') }}
    )
    
    , person as (
        select *
        from {{ ref('stg_person') }}
    )
    
    , store as (
        select *
        from {{ ref('stg_store') }}
    )
    
    , joined as (
        select
            customer.customer_id
            , coalesce(store.store_name, person.full_name) as customer_name
            , person.person_type as customer_type
            , customer.territory_id
            , customer.person_id
            , customer.store_id
            , person.first_name
            , person.last_name
            , store.sales_person_id as store_sales_person_id
        from customer
        left join person 
            on customer.person_id = person.business_entity_id
        left join store 
            on customer.store_id = store.business_entity_id
    )
    
    , final as (
        select
            hash(customer_id) as sk_customer
            , customer_id
            , customer_name
            , customer_type
            , territory_id
            , person_id
            , store_id
            , first_name
            , last_name
            , store_sales_person_id
            , case 
                when person_id is not null then 'Individual'
                when store_id is not null then 'Store'
                else 'Unknown'
            end as customer_category
        from joined
    )

select *
from final