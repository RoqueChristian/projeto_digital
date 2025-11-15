# Projeto de ETL com Airflow: PostgreSQL para SQL Server

Este projeto contém pipelines de dados (ETL) construídos com Apache Airflow para mover e transformar dados de um banco de dados operacional em PostgreSQL para um Data Warehouse em SQL Server.

## Visão Geral

O objetivo principal é popular os fatos de um Data Warehouse a partir de dados transacionais. O projeto utiliza o padrão de extração, transformação e carga (ETL), orquestrado pelo Airflow. Os dados são extraídos do PostgreSQL, transformados em memória usando a biblioteca Pandas e, em seguida, carregados no SQL Server.

O processo de staging (armazenamento temporário) é realizado no sistema de arquivos local do worker do Airflow, utilizando arquivos CSV para passar os dados entre as tarefas de extração, transformação e carga.

## Tecnologias Utilizadas

- **Orquestração:** Apache Airflow
- **Banco de Dados de Origem:** PostgreSQL
- **Banco de Dados de Destino (Data Warehouse):** Microsoft SQL Server
- **Linguagem:** Python 3
- **Bibliotecas Principais:**
  - `apache-airflow-providers-postgres`: Para interagir com o PostgreSQL.
  - `apache-airflow-providers-microsoft-mssql`: Para interagir com o SQL Server.
  - `pandas`: Para manipulação e transformação de dados.
  - `numpy`: Para operações numéricas.

## Pré-requisitos

Para que as DAGs funcionem corretamente, é necessário configurar as seguintes conexões (Connections) na interface do Airflow:

1.  **Conexão com o PostgreSQL (Origem):**
    - **Conn Id:** `postgres_source`
    - **Conn Type:** `Postgres`

2.  **Conexão com o SQL Server (Destino):**
    - **Conn Id:** `mssql_target`
    - **Conn Type:** `Microsoft SQL Server`

## Estrutura do Projeto

O projeto está organizado em DAGs, onde cada DAG é responsável por popular uma tabela de fatos específica no Data Warehouse.

```
/
└── dags/
    ├── fatocontaspagar_dag.py
    └── fatocontasreceber_dag.py
```

## DAGs Implementadas

### 1. `etl_fatocontaspagar_pg_to_mssql`

- **Arquivo:** `dags/fatocontaspagar_dag.py`
- **Objetivo:** Popular a tabela `FatoContasAPagar` no SQL Server.
- **Fonte de Dados:** Tabela `financeiro.conta_pagar` no PostgreSQL.

#### Etapas do Pipeline:

1.  **`extract_postgres_to_file` (Extração):**
    - Conecta-se ao PostgreSQL (`postgres_source`).
    - Executa uma consulta na tabela `financeiro.conta_pagar` para extrair os dados brutos de contas a pagar.
    - Salva o resultado em um arquivo de staging local: `/tmp/contas_pagar_raw.csv`.

2.  **`transform_and_lookup` (Transformação):**
    - Lê o arquivo CSV bruto.
    - Conecta-se ao SQL Server (`mssql_target`) para buscar chaves substitutas das seguintes tabelas de dimensão: `DimSituacaoTitulo`, `DimFormaPagamento` e `DimTempo`.
    - Substitui as chaves de negócio (ex: `id_situacao`) pelas chaves substitutas do Data Warehouse (ex: `id_dim_situacao`).
    - Realiza a limpeza e o tratamento de dados, como a conversão de datas para o formato correto para o lookup.
    - Filtra registros que não encontraram correspondência nas dimensões essenciais para garantir a integridade referencial.
    - Salva o DataFrame transformado em um novo arquivo de staging: `/tmp/contas_pagar_transformed.csv`.

3.  **`load_mssql` (Carga):**
    - Lê o arquivo CSV transformado.
    - **Garante a idempotência:** Antes de inserir, consulta a tabela de destino `FatoContasAPagar` para obter os IDs (`id_conta_pagar`) que já foram carregados e evita a duplicação de registros.
    - Insere apenas os novos registros na tabela de destino.
    - Trata valores nulos (como `id_dim_tempo_pagamento`) para que sejam inseridos como `NULL` no banco de dados.
    - Ao final, remove os arquivos de staging (`.csv`) para limpar o ambiente.

---

### 2. `etl_fatocontasreceber_pg_to_mssql_v2`

- **Arquivo:** `dags/fatocontasreceber_dag.py`
- **Objetivo:** Popular a tabela `FatoContasAReceber` no SQL Server.
- **Fonte de Dados:** Junção das tabelas `financeiro.conta_receber`, `vendas.parcela` e `vendas.nota_fiscal` no PostgreSQL.

#### Etapas do Pipeline:

1.  **`extract_postgres_to_file` (Extração):**
    - Conecta-se ao PostgreSQL (`postgres_source`).
    - Executa uma consulta com `JOIN` entre as tabelas de origem para consolidar as informações de contas a receber.
    - Salva o resultado em um arquivo de staging local: `/tmp/contas_receber_raw.csv`.

2.  **`transform_and_lookup` (Transformação):**
    - Lê o arquivo CSV bruto.
    - Realiza lookups no SQL Server (`mssql_target`) para obter as chaves substitutas das dimensões: `DimSituacaoTitulo`, `DimFormaPagamento`, `DimCliente` e `DimTempo`.
    - Mapeia as chaves de negócio para as chaves substitutas correspondentes.
    - Filtra registros que não possuem correspondência nas dimensões `DimSituacaoTitulo`, `DimCliente` e `DimTempo` (para a data de vencimento).
    - Salva os dados transformados em: `/tmp/contas_receber_transformed.csv`.

3.  **`load_mssql` (Carga):**
    - Lê o arquivo CSV transformado.
    - **Garante a idempotência:** Verifica os registros existentes na tabela `FatoContasAReceber` (baseado na chave de negócio `id_conta_receber`) para evitar duplicações.
    - Utiliza o método `executemany` para uma inserção em lote (bulk insert) dos novos registros, o que melhora a performance.
    - O `INSERT` é construído para funcionar com tabelas que possuem uma chave primária com `IDENTITY`, omitindo a coluna da PK na declaração `INSERT`.
    - Remove os arquivos de staging (`.csv`) após a conclusão da carga.

## Como Executar

1.  Certifique-se de que o Apache Airflow está em execução.
2.  Configure as conexões `postgres_source` e `mssql_target` na UI do Airflow.
3.  Copie os arquivos `dags/*.py` para a pasta de DAGs do seu ambiente Airflow.
4.  Na UI do Airflow, ative as DAGs `etl_fatocontaspagar_pg_to_mssql` e `etl_fatocontasreceber_pg_to_mssql_v2`.
5.  Você pode acioná-las manualmente clicando no botão "Play" ou aguardar a execução agendada (se houver uma configurada).