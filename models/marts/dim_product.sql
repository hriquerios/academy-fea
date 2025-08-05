with
    product as (
        select *
        from {{ ref('stg_products') }}
    )
    
    , category as (
        select *
        from {{ ref('stg_product_category') }}
    )
    
    , subcategory as (
        select *
        from {{ ref('stg_product_subcategory') }}
    )
    
    , model as (
        select *
        from {{ ref('stg_product_model') }}
    )
    
    , joined as (
        select 
            product.product_id
            , product.product_name
            , product.product_number
            , product.list_price
            , product.product_color
            , product.product_size
            , product.product_line
            , product.product_class
            , product.product_style
            , category.product_category_name
            , subcategory.product_subcategory_name
            , model.product_model_name
            , category.product_category_id
            , subcategory.product_subcategory_id
            , model.product_model_id
        from product
        left join subcategory
            on product.product_subcategory_id = subcategory.product_subcategory_id
        left join category
            on subcategory.product_category_id = category.product_category_id
        left join model
            on product.product_model_id = model.product_model_id
    )
    
    , final as (
        select
            hash(product_id) as sk_product
            , product_id
            , product_name
            , product_number
            , list_price
            , product_color
            , product_size
            , product_line
            , product_class
            , product_style
            , product_category_name
            , product_subcategory_name
            , product_model_name
            , product_category_id
            , product_subcategory_id
            , product_model_id
            , concat(product_category_name, ' - ', product_subcategory_name) as category_subcategory
            , concat(product_name, ' (', product_number, ')') as product_display_name
        from joined
    )

select *
from final