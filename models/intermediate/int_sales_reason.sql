with
    sales_reason as (
        select *
        from {{ ref('stg_sales_reason') }}
    )
    
    , order_reason_bridge as (
        select *
        from {{ ref('stg_sales_order_header_sales_reason') }}
    )
    
    , reason_details as (
        select
            order_reason_bridge.sales_order_id
            , sales_reason.sales_reason_name
            , sales_reason.sales_reason_type
        from order_reason_bridge
        left join sales_reason
            on order_reason_bridge.sales_reason_id = sales_reason.sales_reason_id
    )
    
    , aggregated_reasons as (
        select
            sales_order_id
            , listagg(sales_reason_name, ', ') within group (order by sales_reason_name) as reason_names_agg
            , listagg(sales_reason_type, ', ') within group (order by sales_reason_type) as reason_types_agg
            , max(case when lower(sales_reason_type) = 'promotion' then 1 else 0 end) as has_promotion
            , max(case when lower(sales_reason_type) = 'marketing' then 1 else 0 end) as has_marketing
            , max(case when lower(sales_reason_type) = 'other' then 1 else 0 end) as has_other
        from reason_details
        group by sales_order_id
    )

select *
from aggregated_reasons