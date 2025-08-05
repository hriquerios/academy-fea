-- Teste para validar vendas brutas de 2011 conforme solicitado pelo CEO
select 
    sum(gross_total) as vendas_brutas_2011
from {{ ref('fact_sales') }}
where year(order_date) = 2011
having sum(gross_total) != 12646112.16