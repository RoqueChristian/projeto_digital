from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"




@dag(
    dag_id="etl_dimformapagamento_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "forma_pagamento"],
)
def etl_dim_forma_pagamento():
    
    # Consulta PostgreSQL: select distinct fp.id as id_forma_pagamento, fp.descricao as descricao_forma_pagamento from vendas.forma_pagamento fp;
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT DISTINCT 
                fp.id AS id_forma_pagamento, 
                fp.descricao AS descricao_forma_pagamento 
            FROM vendas.forma_pagamento fp;
        """
        print("✅ Extraindo DimFormaPagamento do PostgreSQL.")
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Padronização de colunas
        df.columns = ["id_forma_pagamento", "descricao_forma_pagamento"]
        df["descricao_forma_pagamento"] = df["descricao_forma_pagamento"].astype(str).str.strip()
        print(f"✅ Transformação concluída. {len(df)} registros para carregar.")
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # 1. Cria a tabela temporária (Staging)
        cursor.execute("IF OBJECT_ID('tempdb..#FormaPagtoStaging') IS NOT NULL DROP TABLE #FormaPagtoStaging;")
        cursor.execute("""
            CREATE TABLE #FormaPagtoStaging (
                id_forma_pagamento INT NOT NULL PRIMARY KEY,
                descricao_forma_pagamento VARCHAR(100)
            );
        """)
        
        # 2. Insere os dados na tabela temporária
        insert_sql = "INSERT INTO #FormaPagtoStaging (id_forma_pagamento, descricao_forma_pagamento) VALUES (%s, %s)"
        data = [(row["id_forma_pagamento"], row["descricao_forma_pagamento"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # 3. Aplica o MERGE (UPSERT)
        print(f"✅ Aplicando MERGE de {len(rows)} registros na DimFormaPagamento...")
        merge_sql = """
            MERGE DimFormaPagamento AS Target
            USING #FormaPagtoStaging AS Source
            ON (Target.id_forma_pagamento = Source.id_forma_pagamento)
            
            -- Se a chave existe, atualiza os atributos (SCD Tipo 1)
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.descricao_forma_pagamento = Source.descricao_forma_pagamento
                    
            -- Se a chave não existe, insere a nova linha
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_forma_pagamento, descricao_forma_pagamento)
                VALUES (Source.id_forma_pagamento, Source.descricao_forma_pagamento);
        """
        cursor.execute(merge_sql)
        
        # 4. Finaliza
        conn.commit()
        cursor.execute("DROP TABLE #FormaPagtoStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimFormaPagamento."

    # ORQUESTRAÇÃO
    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)


etl_dim_forma_pagamento()