with 
    source_data as (
        select *
        from {{ source('source_sales', 'sales_salesorderheader') }}
    )
    
    , renamed as (
        select
            cast(salesorderid as integer) as sales_order_id
            , cast(customerid as integer) as customer_id
            , cast(salespersonid as integer) as sales_person_id
            , cast(territoryid as integer) as territory_id
            , cast(billtoaddressid as integer) as bill_to_address_id
            , cast(shiptoaddressid as integer) as ship_to_address_id
            , cast(shipmethodid as integer) as ship_method_id
            , cast(creditcardid as integer) as credit_card_id
            , cast(currencyrateid as integer) as currency_rate_id
            , cast(orderdate as date) as order_date
            , cast(duedate as date) as due_date
            , cast(shipdate as date) as ship_date
            , cast(status as integer) as order_status
            , cast(onlineorderflag as boolean) as online_order_flag
            , cast(accountnumber as varchar(15)) as account_number
            , cast(creditcardapprovalcode as varchar(15)) as credit_card_approval_code
            , cast(subtotal as numeric(19,4)) as subtotal
            , cast(taxamt as numeric(19,4)) as tax_amount
            , cast(freight as numeric(19,4)) as freight
            , cast(totaldue as numeric(19,4)) as total_due
            , cast(rowguid as varchar) as row_guid
            , cast(modifieddate as timestamp) as modified_date
        from source_data
    )
    
select *
from renamed