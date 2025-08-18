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
    
    , person_creditcard as (
        select 
            business_entity_id
            , count(credit_card_id) as total_credit_cards
            , listagg(cast(credit_card_id as varchar), ', ') within group (order by credit_card_id) as credit_card_ids
        from {{ ref('stg_person_credit_card') }}
        group by business_entity_id
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
            , coalesce(person_creditcard.total_credit_cards, 0) as total_credit_cards
            , person_creditcard.credit_card_ids
        from customer
        left join person 
            on customer.person_id = person.business_entity_id
        left join store 
            on customer.store_id = store.business_entity_id
        left join person_creditcard
            on customer.person_id = person_creditcard.business_entity_id
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
            , total_credit_cards
            , credit_card_ids
            , case 
                when person_id is not null then 'Individual'
                when store_id is not null then 'Store'
                else 'Unknown'
            end as customer_category
            , case
                when total_credit_cards = 0 then 'No Credit Cards'
                when total_credit_cards = 1 then 'Single Credit Card'
                when total_credit_cards <= 3 then 'Multiple Credit Cards'
                else 'Many Credit Cards'
            end as credit_card_category
        from joined
    )

select *
from final