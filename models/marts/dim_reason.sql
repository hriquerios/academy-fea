with
    sales_reason as (
        select *
        from {{ ref('int_sales_reason') }}
    )
    
    , final as (
        select
            hash(sales_order_id) as sk_reason
            , sales_order_id
            , reason_names_agg
            , reason_types_agg
            , has_promotion
            , has_marketing
            , has_other
            , case
                when has_promotion = 1 then 'Has Promotion'
                else 'No Promotion'
            end as promotion_flag
            , case
                when has_marketing = 1 then 'Has Marketing'
                else 'No Marketing'  
            end as marketing_flag
            , case
                when has_other = 1 then 'Has Other'
                else 'No Other'
            end as other_flag
        from sales_reason
    )

select *
from final