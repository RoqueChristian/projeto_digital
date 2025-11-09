from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Importe o Hook base do MSSQL para garantir que o conector pyodbc seja carregado corretamente
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# Configuração de Logging para melhor visibilidade
log = logging.getLogger(__name__)

# Nome dos IDs de Conexão (Variáveis globais)
POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="01_check_db_connections",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["teste", "conexao", "saude"],
    doc_md=__doc__, # Adiciona o docstring como descrição na UI
)
def check_db_connections_dag():
    """
    ## Teste de Conexão com Bancos de Dados
    DAG essencial para verificar a conectividade do Airflow com:
    - PostgreSQL de origem (via ID: postgres_source)
    - SQL Server de destino (via ID: mssql_target)
    """

    @task
    def check_postgres_source_connection():
        """Verifica a conexão com o PostgreSQL de Origem."""
        log.info(f"Tentando conectar ao PostgreSQL com Conn ID: {POSTGRES_CONN_ID}")
        
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            # Execute uma consulta simples
            pg_hook.run("SELECT 1", autocommit=True)
            log.info("✅ Conexão com o PostgreSQL de Origem BEM-SUCEDIDA.")
        except Exception as e:
            # log.exception(e) registra o erro e o stack trace completo
            log.exception(f"❌ ERRO CRÍTICO ao conectar ao PostgreSQL com Conn ID '{POSTGRES_CONN_ID}'.")
            raise e

    @task
    def check_mssql_target_connection():
        """Verifica a conexão com o SQL Server de Destino."""
        log.info(f"Tentando conectar ao SQL Server com Conn ID: {MSSQL_CONN_ID}")
        
        try:
            mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
            # Execute uma consulta simples
            # Se o banco de dados for muito lento, considere um timeout aqui
            mssql_hook.run("SELECT 1", autocommit=True) 
            log.info("✅ Conexão com o SQL Server de Destino BEM-SUCEDIDA.")
        except Exception as e:
            # log.exception(e) captura a falha de login (Erro 18456) e exibe o stack trace completo
            log.exception(f"❌ ERRO CRÍTICO ao conectar ao SQL Server com Conn ID '{MSSQL_CONN_ID}'.")
            raise e

    # Define a ordem de execução das tarefas
    check_postgres_source_connection() >> check_mssql_target_connection()

# Instancia o DAG
check_db_connections_dag()