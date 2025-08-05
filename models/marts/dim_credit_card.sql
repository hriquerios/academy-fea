with 
    credit_card as (
        select
            hash(credit_card_id) as sk_creditcard
            , credit_card_id
            , card_type
            , modified_date
        from {{ ref('stg_credit_card') }}
    )
    
select *
from credit_card