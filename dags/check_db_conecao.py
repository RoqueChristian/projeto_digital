from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Nome dos IDs de Conexão configurados na UI do Airflow
POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="01_check_db_connections",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["teste", "conexao"],
)
def check_db_connections_dag():
    """
    DAG para testar a conectividade com o PostgreSQL de origem e o SQL Server de destino.
    """

    @task
    def check_postgres_source_connection():
        """Verifica a conexão com o PostgreSQL de Origem."""
        print(f"Tentando conectar ao PostgreSQL com Conn ID: {POSTGRES_CONN_ID}")
        
        # Cria uma instância do Hook usando o ID de Conexão
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Executa uma consulta de teste
        try:
            pg_hook.run("SELECT 1", autocommit=True)
            print("✅ Conexão com o PostgreSQL de Origem BEM-SUCEDIDA.")
        except Exception as e:
            print(f"❌ ERRO ao conectar ao PostgreSQL de Origem: {e}")
            raise

    @task
    def check_mssql_target_connection():
        """Verifica a conexão com o SQL Server de Destino."""
        print(f"Tentando conectar ao SQL Server com Conn ID: {MSSQL_CONN_ID}")
        
        # Cria uma instância do Hook usando o ID de Conexão
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        # Executa uma consulta de teste
        try:
            mssql_hook.run("SELECT 1", autocommit=True)
            print("✅ Conexão com o SQL Server de Destino BEM-SUCEDIDA.")
        except Exception as e:
            # Em caso de falha, exibe o erro completo para debug
            print(f"❌ ERRO ao conectar ao SQL Server de Destino: {e}")
            raise

    # Define a ordem de execução das tarefas
    check_postgres_source_connection() >> check_mssql_target_connection()

# Instancia o DAG
check_db_connections_dag()