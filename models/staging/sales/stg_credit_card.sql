with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_creditcard') }}
    )
    
    , renamed as (
        select
            cast(creditcardid as integer) as credit_card_id
            , cast(cardtype as varchar(50)) as card_type
            , cast(cardnumber as varchar(25)) as card_number
            , cast(expmonth as integer) as expiration_month
            , cast(expyear as integer) as expiration_year
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed