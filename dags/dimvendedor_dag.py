from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"


@dag(
    dag_id="etl_dimvendedor_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "vendedor"],
)
def etl_dim_vendedor():
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT DISTINCT 
                pf.id AS id_vendedor, 
                pf.nome AS nome_vendedor, 
                pf.cpf 
            FROM geral.pessoa_fisica pf;
        """
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Renomeação e Limpeza
        df.columns = ["id_vendedor", "nome_vendedor", "cpf"]
        df["nome_vendedor"] = df["nome_vendedor"].astype(str).str.strip()
        df["cpf"] = df["cpf"].astype(str).str.strip()
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # Tabela temporária
        cursor.execute("IF OBJECT_ID('tempdb..#VendedorStaging') IS NOT NULL DROP TABLE #VendedorStaging;")
        cursor.execute("""
            CREATE TABLE #VendedorStaging (
                id_vendedor INT NOT NULL PRIMARY KEY,
                nome_vendedor VARCHAR(255),
                cpf VARCHAR(14)
            );
        """)
        
        # Insere na temporária
        insert_sql = "INSERT INTO #VendedorStaging (id_vendedor, nome_vendedor, cpf) VALUES (%s, %s, %s)"
        data = [(row["id_vendedor"], row["nome_vendedor"], row["cpf"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # Aplica o MERGE
        merge_sql = """
            MERGE DimVendedor AS Target
            USING #VendedorStaging AS Source
            ON (Target.id_vendedor = Source.id_vendedor)
            
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.nome_vendedor = Source.nome_vendedor,
                    Target.cpf = Source.cpf
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_vendedor, nome_vendedor, cpf)
                VALUES (Source.id_vendedor, Source.nome_vendedor, Source.cpf);
        """
        cursor.execute(merge_sql)
        
        conn.commit()
        cursor.execute("DROP TABLE #VendedorStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimVendedor."

    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)

etl_dim_vendedor()