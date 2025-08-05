# 🚴‍♂️ Adventure Works Analytics - Certificação Indicium
# 📋 Sobre o Projeto
Projeto de Engenharia de Analytics desenvolvido para a certificação da Indicium, aplicando a metodologia Modern Analytics Stack na empresa Adventure Works, uma indústria de bicicletas com mais de 500 produtos, 20.000 clientes e 31.000 pedidos.
# 🎯 Objetivo
Construir uma infraestrutura moderna de analytics end-to-end para transformar a Adventure Works em uma empresa data-driven, com foco inicial na área de vendas.
# 🏗️ Arquitetura
Raw Data (Snowflake) → dbt → Marts → BI Dashboard
# Stack Tecnológica
Data Warehouse: Snowflake

Transformação: dbt Cloud

Versionamento: GitHub

BI: Power BI 

## 🔧 Como Executar

```bash
# Instalar dependências
dbt deps

# Executar transformações
dbt run

# Executar testes
dbt test

# Gerar documentação
dbt docs generate
