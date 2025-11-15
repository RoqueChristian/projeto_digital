from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="etl_dimproduto_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "produto"],
)
def etl_dim_produto():
    
    # Consulta: select distinct p.id as id_produto, p.id_fornecedor, c.descricao as categoria, p.nome as descricao_produto from vendas.produto p inner join vendas.categoria c on c.id = p.id_categoria;
    
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT DISTINCT  
                p.id AS id_produto, 
                p.id_fornecedor, 
                c.descricao AS categoria, 
                p.nome AS descricao_produto 
            FROM vendas.produto p
            INNER JOIN vendas.categoria c ON c.id = p.id_categoria;
        """
        return pg.get_pandas_df(sql).to_dict(orient="records")

    @task
    def transform_data(rows):
        if not rows: return []
        df = pd.DataFrame(rows)
        # Renomeação e Limpeza
        df.columns = ["id_produto", "id_fornecedor", "categoria", "descricao_produto"]
        df["categoria"] = df["categoria"].astype(str).str.strip()
        df["descricao_produto"] = df["descricao_produto"].astype(str).str.strip()
        return df.to_dict(orient="records")

    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # Usamos uma tabela temporária para aplicar a lógica de MERGE (upsert)
        cursor.execute("IF OBJECT_ID('tempdb..#ProdutoStaging') IS NOT NULL DROP TABLE #ProdutoStaging;")
        cursor.execute("""
            CREATE TABLE #ProdutoStaging (
                id_produto INT NOT NULL PRIMARY KEY,
                id_fornecedor INT,
                categoria VARCHAR(100),
                descricao_produto VARCHAR(255)
            );
        """)
        
        # Insere os dados na tabela temporária
        insert_sql = "INSERT INTO #ProdutoStaging (id_produto, id_fornecedor, categoria, descricao_produto) VALUES (%s, %s, %s, %s)"
        data = [(row["id_produto"], row["id_fornecedor"], row["categoria"], row["descricao_produto"]) for row in rows]
        cursor.executemany(insert_sql, data)
        
        # Aplica o MERGE
        print(f"✅ Aplicando MERGE de {len(rows)} registros na DimProduto...")
        merge_sql = """
            MERGE DimProduto AS Target
            USING #ProdutoStaging AS Source
            ON (Target.id_produto = Source.id_produto)
            
            WHEN MATCHED THEN
                UPDATE SET 
                    Target.id_fornecedor = Source.id_fornecedor,
                    Target.categoria = Source.categoria,
                    Target.descricao_produto = Source.descricao_produto
                    
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (id_produto, id_fornecedor, categoria, descricao_produto)
                VALUES (Source.id_produto, Source.id_fornecedor, Source.categoria, Source.descricao_produto);
        """
        cursor.execute(merge_sql)
        
        conn.commit()
        cursor.execute("DROP TABLE #ProdutoStaging;")
        return f"{len(rows)} registros processados e carregados via MERGE na DimProduto."

    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)

etl_dim_produto()