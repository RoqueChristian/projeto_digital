from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="etl_dimsituacaotitulo_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "situacao_titulo"],
)
def etl_dim_situacao_titulo():
    
    # Consulta PostgreSQL: select distinct st.id as id_situacao, st.descricao as situacao_titulo from financeiro.situacao_titulo st;
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT DISTINCT 
                st.id AS id_situacao, 
                st.descricao AS situacao_titulo 
            FROM 
                financeiro.situacao_titulo st;
        """
        print("✅ Extraindo DimSituacaoTitulo do PostgreSQL.")
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Padronização de colunas
        df.columns = ["id_situacao", "situacao_titulo"]
        df["situacao_titulo"] = df["situacao_titulo"].astype(str).str.strip()
        print(f"✅ Transformação concluída. {len(df)} registros para carregar.")
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # 1. Cria a tabela temporária (Staging)
        cursor.execute("IF OBJECT_ID('tempdb..#SituacaoStaging') IS NOT NULL DROP TABLE #SituacaoStaging;")
        cursor.execute("""
            CREATE TABLE #SituacaoStaging (
                id_situacao INT NOT NULL PRIMARY KEY,
                situacao_titulo VARCHAR(100)
            );
        """)
        
        # 2. Insere os dados na tabela temporária
        insert_sql = "INSERT INTO #SituacaoStaging (id_situacao, situacao_titulo) VALUES (%s, %s)"
        data = [(row["id_situacao"], row["situacao_titulo"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # 3. Aplica o MERGE (UPSERT)
        print(f"✅ Aplicando MERGE de {len(rows)} registros na DimSituacaoTitulo...")
        merge_sql = """
            MERGE DimSituacaoTitulo AS Target
            USING #SituacaoStaging AS Source
            ON (Target.id_situacao = Source.id_situacao)
            
            -- Se a chave existe, atualiza os atributos (SCD Tipo 1)
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.situacao_titulo = Source.situacao_titulo
                    
            -- Se a chave não existe, insere a nova linha
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_situacao, situacao_titulo)
                VALUES (Source.id_situacao, Source.situacao_titulo);
        """
        cursor.execute(merge_sql)
        
        # 4. Finaliza
        conn.commit()
        cursor.execute("DROP TABLE #SituacaoStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimSituacaoTitulo."

    # ORQUESTRAÇÃO
    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)


etl_dim_situacao_titulo()