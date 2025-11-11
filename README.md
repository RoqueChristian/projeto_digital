# Projeto Digital - ETL de Situação Financeira

Este projeto implementa um pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow para migrar dados de uma tabela de situação financeira de um banco de dados PostgreSQL para um SQL Server.

## Tecnologias Utilizadas

- **Apache Airflow**: Orquestração e agendamento do pipeline ETL.
- **Python**: Linguagem de programação para o desenvolvimento da DAG e lógica de transformação.
- **Pandas**: Biblioteca Python para manipulação e transformação de dados.
- **PostgreSQL**: Banco de dados de origem.
- **Microsoft SQL Server**: Banco de dados de destino.
- **Docker & Docker Compose**: Para containerização e gerenciamento do ambiente de desenvolvimento.

## Estrutura do Projeto

```
projeto_digital/
├── dags/
│   └── etl_situacao_pg_mssql.py
├── docker-compose.yaml
├── .env (arquivo de variáveis de ambiente - não versionado)
├── logs/
├── plugins/
└── data/
```

- `dags/`: Contém os arquivos das DAGs do Airflow.
- `docker-compose.yaml`: Define os serviços Docker para Airflow (webserver, scheduler, init) e PostgreSQL.
- `.env`: Arquivo para variáveis de ambiente sensíveis (ex: senhas de banco de dados).
- `logs/`: Diretório para logs do Airflow.
- `plugins/`: Diretório para plugins personalizados do Airflow.
- `data/`: Diretório para dados temporários ou persistentes.

## DAG: `etl_situacao_pg_to_mssql`

A DAG `etl_situacao_pg_to_mssql` é responsável por extrair dados da tabela `financeiro.situacao_titulo` do PostgreSQL, transformá-los e carregá-los na tabela `DimSituacao` no SQL Server.

### Fluxo da DAG

1.  **`extract_postgres`**:
    *   Conecta-se ao PostgreSQL (usando `POSTGRES_CONN_ID`).
    *   Extrai `id` e `descricao` da tabela `financeiro.situacao_titulo`.
    *   Retorna os dados como um DataFrame Pandas.

2.  **`transform_data`**:
    *   Recebe os dados extraídos.
    *   Padroniza os nomes das colunas para `ID_Situacao_Origem` e `Descricao_Situacao`.
    *   Remove espaços em branco e normaliza o texto da coluna `Descricao_Situacao`.
    *   Retorna os dados transformados.

3.  **`load_mssql`**:
    *   Conecta-se ao SQL Server (usando `MSSQL_CONN_ID`).
    *   Insere os registros transformados na tabela `DimSituacao`.
    *   A tabela `DimSituacao` espera as colunas `ID_Situacao_Origem` e `Descricao_Situacao`.

### Conexões do Airflow

As conexões são configuradas via variáveis de ambiente no `docker-compose.yaml` (e idealmente no arquivo `.env` para valores sensíveis).

-   **`POSTGRES_CONN_ID` (ID: `postgres_source`)**:
    *   Tipo: PostgreSQL
    *   Configuração no `docker-compose.yaml`:
        ```yaml
        AIRFLOW_CONN_POSTGRES_SOURCE: >
          postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:5432/${POSTGRES_DB}
        ```

-   **`MSSQL_CONN_ID` (ID: `mssql_target`)**:
    *   Tipo: MS SQL Server
    *   Configuração no `docker-compose.yaml`:
        ```yaml
        AIRFLOW_CONN_MSSQL_TARGET: >
          mssql+pyodbc://sa:${MSSQL_PASSWORD}@host.docker.internal:1450/DataWarehouse
          ?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
        ```
    *   **Nota**: A conexão com o SQL Server utiliza `host.docker.internal` para acessar um serviço rodando na máquina host (Windows, porta 1450). O driver `ODBC Driver 18 for SQL Server` é especificado, e `TrustServerCertificate=yes` é usado para simplificar a conexão em ambientes de desenvolvimento.

## Como Rodar o Projeto

1.  **Configurar o arquivo `.env`**:
    Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis (exemplo):
    ```
    POSTGRES_USER=airflow
    POSTGRES_PASSWORD=airflow
    POSTGRES_HOST=postgres
    POSTGRES_DB=airflow
    MSSQL_PASSWORD=SuaSenhaDoSQLServer
    AIRFLOW_ADMIN_USER=admin
    AIRFLOW_ADMIN_PASSWORD=admin
    AIRFLOW_ADMIN_EMAIL=admin@example.com
    ```
    Certifique-se de que `MSSQL_PASSWORD` corresponda à senha do usuário `sa` do seu SQL Server.

2.  **Iniciar os serviços Docker**:
    Navegue até o diretório raiz do projeto e execute:
    ```bash
    docker-compose up -d
    ```
    Isso irá iniciar o PostgreSQL, o Airflow (webserver, scheduler e o serviço de inicialização).

3.  **Acessar a UI do Airflow**:
    Após os serviços estarem em execução, você pode acessar a interface do usuário do Airflow em `http://localhost:8080`.
    Faça login com as credenciais definidas no seu arquivo `.env` (ex: `admin`/`admin`).

4.  **Despausar e Executar a DAG**:
    Na UI do Airflow, localize a DAG `etl_situacao_pg_to_mssql`. Despause-a e, em seguida, acione-a manualmente para iniciar o pipeline ETL.

## Pré-requisitos

- Docker Desktop (ou Docker Engine e Docker Compose) instalado.
- Um servidor SQL Server acessível na máquina host (ou em outro contêiner/servidor) na porta 1450, com um banco de dados `DataWarehouse` e um usuário `sa` configurado.
- O driver ODBC `ODBC Driver 18 for SQL Server` deve estar disponível no ambiente onde o contêiner do Airflow será executado (o `Dockerfile` base deve incluir a instalação, se necessário, mas o `docker-compose.yaml` já aponta para a imagem `build: .`, o que implica que a imagem é construída localmente e deve conter os pré-requisitos).

## Considerações

-   **Segurança**: Para ambientes de produção, evite colocar senhas diretamente no `.env` ou `docker-compose.yaml`. Considere usar um sistema de gerenciamento de segredos.
-   **Tratamento de Erros**: A DAG atual tem tratamento básico de erros. Para produção, adicione mais robustez (retries, alerts, etc.).
-   **Idempotência**: A DAG atual insere dados. Se executada múltiplas vezes, pode duplicar registros. Considere estratégias de upsert (MERGE) ou deleção antes da inserção para garantir idempotência.
```