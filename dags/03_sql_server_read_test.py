from __future__ import annotations

import logging
import pendulum
import pyodbc # CRÍTICO: Importa a biblioteca de driver
from typing import List, Tuple

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook # Para obter as credenciais

# =========================================================================
# 1. CONFIGURAÇÕES CRÍTICAS
# =========================================================================

MSSQL_CONN_ID = "mssql_target"
TARGET_TABLE = "[DW].[DimSituacao]"
SQL_QUERY_READ = f"SELECT * FROM {TARGET_TABLE};"

# =========================================================================

@dag(
    dag_id="03_SQL_SERVER_READ_TEST_PYODBC", # Novo ID para garantir nova execução
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["teste", "mssql", "pyodbc"],
)
def sql_server_read_pyodbc_dag():

    @task(task_id="read_dim_situacao_pyodbc")
    def read_data_pyodbc(sql_query: str, target_table: str):
        """
        Executa um SELECT simples no SQL Server usando pyodbc puro para isolar
        o problema de driver/provider do Airflow Hook.
        """
        # 1. Obtém as credenciais de conexão do Airflow
        mssql_conn = BaseHook.get_connection(MSSQL_CONN_ID)
        
        # 2. Constrói a string de conexão ODBC (usando Driver 17, padrão em Linux)
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={mssql_conn.host},{mssql_conn.port or 1433};"
            f"DATABASE={mssql_conn.schema};"
            f"UID={mssql_conn.login};"
            f"PWD={mssql_conn.password};"
        )
        
        logging.info(f"Iniciando consulta pyodbc para: {target_table}")
        
        try:
            # 3. Estabelece a conexão e executa a consulta
            cnxn = pyodbc.connect(conn_str)
            cursor = cnxn.cursor()
            
            logging.info("✅ Conexão pyodbc estabelecida. Executando SELECT...")
            
            cursor.execute(sql_query)
            
            # 4. Obtém os registros
            data: List[Tuple] = cursor.fetchall()
            cnxn.close()
            
            if not data:
                logging.warning(f"Nenhum dado encontrado em {target_table}. (Tabela vazia)")
                return

            logging.info(f"✅ Consulta BEM-SUCEDIDA! {len(data)} linhas encontradas.")
            
            # Loga as colunas e as 5 primeiras linhas para visualização
            logging.info(f"Colunas: ['ID_Situacao', 'Descricao_Situacao']")
            for i, row in enumerate(data[:5]):
                logging.info(f"Linha {i+1}: {row}")
                
            if len(data) > 5:
                logging.info("...")
                
        except pyodbc.Error as e:
            # Captura o erro específico do driver
            logging.error(f"❌ FALHA CRÍTICA NO PYODBC/ODBC. Traceback completo:")
            logging.error(e)
            raise 
        except Exception as e:
            # Captura qualquer outra exceção de código
            logging.error(f"❌ FALHA DE EXECUÇÃO: {e}")
            raise
    
    # DEFINE O FLUXO: Apenas uma tarefa
    read_data_pyodbc(SQL_QUERY_READ, TARGET_TABLE)


# Instancia o DAG
sql_server_read_pyodbc_dag()