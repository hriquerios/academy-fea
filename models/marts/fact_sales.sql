with
    order_sales as (
        select *
        from {{ ref('int_sales_order') }}
    )
    
    , creditcard as (
        select *
        from {{ ref('dim_credit_card') }}
    )
    
    , customer as (
        select *
        from {{ ref('dim_customer') }}
    )
    
    , date_dim as (
        select *
        from {{ ref('dim_date') }}
    )
    
    , location as (
        select *
        from {{ ref('dim_location') }}
    )
    
    , product as (
        select *
        from {{ ref('dim_product') }}
    )
    
    , reason as (
        select *
        from {{ ref('dim_reason') }}
    )
    
    , fact_sales as (
        select
            order_sales.sales_order_detail_id as pk_fact_sales
            , creditcard.sk_creditcard as fk_creditcard
            , customer.sk_customer as fk_customer
            , date_dim.sk_date as fk_date
            , location.sk_location as fk_location
            , product.sk_product as fk_product
            , reason.sk_reason as fk_reason
            , order_sales.sales_order_id
            , order_sales.sales_order_detail_id
            , order_sales.product_id
            , order_sales.customer_id
            , order_sales.order_date
            , order_sales.due_date
            , order_sales.ship_date
            , product.product_name
            , product.product_category_name
            , product.product_subcategory_name  
            , customer.customer_name
            , customer.customer_type
            , customer.customer_category
            , location.city_name
            , location.state_province_name
            , location.country_name
            , location.territory_name
            , location.territory_group
            , creditcard.card_type
            , reason.reason_names_agg as sales_reason_names
            , reason.reason_types_agg as sales_reason_types
            , reason.has_promotion
            , reason.has_marketing
            , reason.has_other
            , reason.promotion_flag
            , case
                when order_sales.order_status = 1 then 'In Process'
                when order_sales.order_status = 2 then 'Approved'
                when order_sales.order_status = 3 then 'Backordered'
                when order_sales.order_status = 4 then 'Rejected'
                when order_sales.order_status = 5 then 'Shipped'
                when order_sales.order_status = 6 then 'Cancelled'
                else 'Unknown'
            end as order_status_desc
            , order_sales.order_quantity
            , order_sales.unit_price
            , order_sales.unit_price_discount
            , order_sales.tax_amount_allocated
            , order_sales.freight_allocated
            , order_sales.gross_total
            , order_sales.net_total
            , order_sales.total_product_value
            , order_sales.gross_total as total_sales_amount
            , order_sales.order_quantity as quantity_sold
            
        from order_sales
        left join creditcard 
            on order_sales.credit_card_id = creditcard.credit_card_id
        left join customer 
            on order_sales.customer_id = customer.customer_id
        left join date_dim 
            on order_sales.order_date = date_dim.date_day
        left join location 
            on order_sales.bill_to_address_id = location.address_id
        left join product 
            on order_sales.product_id = product.product_id
        left join reason 
            on order_sales.sales_order_id = reason.sales_order_id
    )

select *
from fact_sales