with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesorderdetail') }}
    )
    
    , renamed as (
        select
            cast(salesorderdetailid as integer) as sales_order_detail_id
            , cast(salesorderid as integer) as sales_order_id
            , cast(productid as integer) as product_id
            , cast(specialofferid as integer) as special_offer_id
            , cast(carriertrackingnumber as varchar(25)) as carrier_tracking_number
            , cast(orderqty as integer) as order_quantity
            , cast(unitprice as numeric(19,4)) as unit_price
            , cast(unitpricediscount as numeric(5,2)) as unit_price_discount
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed