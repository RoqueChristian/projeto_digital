from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"



@dag(
    dag_id="etl_dimcliente_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "cliente"],
)
def etl_dim_cliente():
    
    # Consulta: dimcliente (com UNION ALL)
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            WITH clientes AS (
                SELECT 
                    id, nome AS cliente, 'fisica' AS tipo
                FROM geral.pessoa_fisica
                UNION ALL
                SELECT 
                    id, razao_social AS cliente, 'juridica' AS tipo
                FROM geral.pessoa_juridica
            )
            SELECT DISTINCT
                c.id AS id_cliente,
                c.cliente,
                c.tipo AS tipo_cliente
            FROM clientes c;
        """
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Renomeação e Limpeza
        df.columns = ["id_cliente", "cliente", "tipo_cliente"]
        df["cliente"] = df["cliente"].astype(str).str.strip()
        df["tipo_cliente"] = df["tipo_cliente"].astype(str).str.strip().str.capitalize()
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # Tabela temporária
        cursor.execute("IF OBJECT_ID('tempdb..#ClienteStaging') IS NOT NULL DROP TABLE #ClienteStaging;")
        cursor.execute("""
            CREATE TABLE #ClienteStaging (
                id_cliente INT NOT NULL PRIMARY KEY,
                cliente VARCHAR(255),
                tipo_cliente VARCHAR(10)
            );
        """)
        
        # Insere na temporária
        insert_sql = "INSERT INTO #ClienteStaging (id_cliente, cliente, tipo_cliente) VALUES (%s, %s, %s)"
        data = [(row["id_cliente"], row["cliente"], row["tipo_cliente"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # Aplica o MERGE
        merge_sql = """
            MERGE DimCliente AS Target
            USING #ClienteStaging AS Source
            ON (Target.id_cliente = Source.id_cliente)
            
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.cliente = Source.cliente,
                    Target.tipo_cliente = Source.tipo_cliente
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_cliente, cliente, tipo_cliente)
                VALUES (Source.id_cliente, Source.cliente, Source.tipo_cliente);
        """
        cursor.execute(merge_sql)
        
        conn.commit()
        cursor.execute("DROP TABLE #ClienteStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimCliente."

    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)

etl_dim_cliente()