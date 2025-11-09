from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from typing import List, Tuple

# =========================================================================
# 1. CONFIGURAÇÕES CRÍTICAS
# =========================================================================

MSSQL_CONN_ID = "mssql_target"
TARGET_TABLE = "DW.DimSituacao"
SQL_QUERY_READ = f"SELECT ID_Situacao, Descricao_Situacao FROM {TARGET_TABLE};"

# =========================================================================

@dag(
    dag_id="03_SQL_SERVER_READ_TEST",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["teste", "mssql", "read"],
)
def sql_server_read_dag():

    @task(task_id="read_dim_situacao_and_log")
    def read_data_from_mssql(sql_query: str, target_table: str):
        """
        Executa um SELECT simples no SQL Server e loga o resultado.
        """
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        logging.info(f"Iniciando consulta de leitura no SQL Server para: {target_table}")

        # Executa o SELECT e retorna os registros (lista de tuplas)
        # O método get_records() é a maneira mais segura de ler dados no Airflow
        data: List[Tuple] = mssql_hook.get_records(sql_query)
        
        if not data:
            logging.warning(f"Nenhum dado encontrado em {target_table}.")
            return

        logging.info(f"✅ Consulta BEM-SUCEDIDA! {len(data)} linhas encontradas.")
        
        # Loga as colunas e as 5 primeiras linhas para visualização
        logging.info(f"Colunas: ['ID_Situacao', 'Descricao_Situacao']")
        for i, row in enumerate(data[:5]):
            logging.info(f"Linha {i+1}: {row}")
            
        if len(data) > 5:
            logging.info("...")
            
    
    # DEFINE O FLUXO: Apenas uma tarefa
    read_data_from_mssql(SQL_QUERY_READ, TARGET_TABLE)


# Instancia o DAG
sql_server_read_dag()