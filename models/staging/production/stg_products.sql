with 
    source_data as (
        select *
        from {{ source('source_production', 'production_product') }}
    )
    
    , renamed as (
        select 
            cast(productid as int) as product_id
            , cast(productsubcategoryid as integer) as product_subcategory_id
            , cast(productmodelid as int) as product_model_id
            , cast(name as varchar(50)) as product_name
            , cast(productnumber as varchar(25)) as product_number
            , cast(finishedgoodsflag as boolean) as finished_goods_flag
            , cast(standardcost as numeric(19,4)) as standard_cost
            , cast(listprice as numeric(19,4)) as list_price
            , cast(size as varchar(5)) as product_size
            , cast(sizeunitmeasurecode as varchar(3)) as size_unit_measure_code
            , cast(weightunitmeasurecode as varchar(3)) as weight_unit_measure_code
            , cast(weight as numeric(8,2)) as product_weight
            , cast(productline as varchar(2)) as product_line
            , cast(class as varchar(2)) as product_class
            , cast(style as varchar(2)) as product_style
            , cast(color as varchar(15)) as product_color
            , cast(sellstartdate as date) as sell_start_date
            , cast(sellenddate as date) as sell_end_date
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed