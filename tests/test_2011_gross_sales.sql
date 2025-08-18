-- Teste para validar vendas brutas de 2011 conforme solicitado pelo CEO
select 
    round(sum(gross_total), 2) as vendas_brutas_2011
from {{ ref('fact_sales') }}
where year(order_date) = 2011
having round(sum(gross_total), 2) != 12646112.16