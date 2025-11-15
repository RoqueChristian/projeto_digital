from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

# ... (Imports Airflow, Hooks, pandas, pendulum) ...

@dag(
    dag_id="etl_dimfornecedor_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "fornecedor"],
)
def etl_dim_fornecedor():
    
    # Consulta: select distinct id as id_fornecedor, razao_social as fornecedor, cnpj as cnpj_fornecedot  from  geral.pessoa_juridica;
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT DISTINCT 
                id AS id_fornecedor, 
                razao_social AS fornecedor, 
                cnpj AS cnpj_fornecedor  
            FROM geral.pessoa_juridica;
        """
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Renomeação e Limpeza
        df.columns = ["id_fornecedor", "fornecedor", "cnpj_fornecedor"]
        df["fornecedor"] = df["fornecedor"].astype(str).str.strip()
        df["cnpj_fornecedor"] = df["cnpj_fornecedor"].astype(str).str.strip()
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # Tabela temporária
        cursor.execute("IF OBJECT_ID('tempdb..#FornecedorStaging') IS NOT NULL DROP TABLE #FornecedorStaging;")
        cursor.execute("""
            CREATE TABLE #FornecedorStaging (
                id_fornecedor INT NOT NULL PRIMARY KEY,
                fornecedor VARCHAR(255),
                cnpj_fornecedor VARCHAR(20)
            );
        """)
        
        # Insere na temporária
        insert_sql = "INSERT INTO #FornecedorStaging (id_fornecedor, fornecedor, cnpj_fornecedor) VALUES (%s, %s, %s)"
        data = [(row["id_fornecedor"], row["fornecedor"], row["cnpj_fornecedor"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # Aplica o MERGE
        merge_sql = """
            MERGE DimFornecedor AS Target
            USING #FornecedorStaging AS Source
            ON (Target.id_fornecedor = Source.id_fornecedor)
            
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.fornecedor = Source.fornecedor,
                    Target.cnpj_fornecedor = Source.cnpj_fornecedor
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_fornecedor, fornecedor, cnpj_fornecedor)
                VALUES (Source.id_fornecedor, Source.fornecedor, Source.cnpj_fornecedor);
        """
        cursor.execute(merge_sql)
        
        conn.commit()
        cursor.execute("DROP TABLE #FornecedorStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimFornecedor."

    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)

etl_dim_fornecedor()