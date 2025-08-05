with
    order_header as (
        select *
        from {{ ref('stg_sales_order_header') }}
    )
    
    , order_detail as (
        select *
        from {{ ref('stg_sales_order_detail') }}
    )
    
    , sales_metrics as (
        select 
            order_detail.sales_order_detail_id
            , order_detail.sales_order_id
            , order_detail.product_id
            , order_header.customer_id
            , order_header.sales_person_id
            , order_header.bill_to_address_id
            , order_header.ship_to_address_id
            , order_header.credit_card_id
            , order_header.order_date
            , order_header.due_date
            , order_header.ship_date
            , order_detail.order_quantity
            , order_detail.unit_price
            , order_detail.unit_price_discount
            , order_header.tax_amount
            , order_header.tax_amount / count(*) over(partition by order_header.sales_order_id) as tax_amount_allocated
            , order_header.freight
            , order_header.freight / count(*) over(partition by order_header.sales_order_id) as freight_allocated
            , order_header.total_due as order_total_due
            , order_header.order_status
            , order_header.credit_card_approval_code
            , order_detail.unit_price * order_detail.order_quantity as gross_total
            , (order_detail.unit_price * order_detail.order_quantity) * (1 - order_detail.unit_price_discount) as net_total
        from order_detail
        left join order_header
            on order_detail.sales_order_id = order_header.sales_order_id
    )
    
    , final_metrics as (
        select *
            , net_total + freight_allocated + tax_amount_allocated as total_product_value
        from sales_metrics 
    )

select *
from final_metrics