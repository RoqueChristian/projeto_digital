from __future__ import annotations

import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from typing import Dict, List, Tuple

# =========================================================================
# 1. CONFIGURAÇÕES DE CONEXÃO E CARGA
# =========================================================================

# IDs de Conexão (Confirmados como BEM-SUCEDIDOS!)
POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

# Configuração para carregar DW.DimSituacao
ETL_CONFIG_SITUACAO: Dict = {
    "target_table": "DW.DimSituacao",
    # As colunas de destino no SQL Server (int, varchar(100))
    "target_columns": ["ID_Situacao", "Descricao_Situacao"],
    
    # Consulta de Extração no Postgres
    "sql_query": """
        select
            -- CRÍTICO: Força a conversão para INTEGER para evitar erro de serialização do driver
            CAST(id AS INTEGER) as ID_Situacao,
            -- CRÍTICO: Força a conversão para VARCHAR(100) para evitar problemas com tipo 'text'
            CAST(descricao AS VARCHAR(100)) as Descricao_Situacao
        from financeiro.situacao_titulo
    """,
}
# =========================================================================

@dag(
    dag_id="02_TESTE_LOAD_DIM_SITUACAO_FINAL_V2", # Novo ID para garantir a execução desta versão
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "teste", "dimensao"],
)
def single_full_load_situacao_dag():

    @task(task_id="load_dim_situacao_final")
    def full_load_situacao(config: Dict):
        """
        Executa a carga da DimSituacao. Converte tuplas para listas para resolver erro de driver pyodbc.
        """
        target_table = config["target_table"]
        target_columns = config["target_columns"]
        sql_query = config["sql_query"]

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        logging.info(f"Iniciando Carga de Diagnóstico para: {target_table}")

        # --- FASE 1: EXTRAÇÃO (E) ---
        # Retorna uma lista de tuplas: [(1, 'EM_ABERTO'), (2, 'ATRASADA'), ...]
        data_tuples: List[Tuple] = pg_hook.get_records(sql_query) 
        
        if not data_tuples:
            logging.warning(f"Nenhum dado encontrado para {target_table}. Abortando.")
            return
            
        # --- FASE 1.5: TRANSFORMAÇÃO (T) - CRÍTICA PARA pyodbc ---
        # Converte a lista de tuplas (imutáveis) em lista de listas (mutáveis), 
        # que é o formato mais estável para o MsSqlHook/pyodbc.
        data_lists: List[List] = [list(row) for row in data_tuples]
        logging.info(f"Dados extraídos: {len(data_lists)} linhas. Convertidos para Listas.")


        # --- FASE 2: CARREGAMENTO (L) ---
        logging.info(f"Carregando {len(data_lists)} linhas na tabela {target_table}...")
        
        # replace=True: Tenta TRUNCATE TABLE antes do INSERT, testando permissão.
        mssql_hook.insert_rows(
            table=target_table, 
            rows=data_lists,  # Passa a lista de listas
            target_fields=target_columns, 
            replace=True # Tenta a carga completa (TRUNCATE & INSERT)
        )
        
        logging.info(f"✅ Carga COMPLETA de {len(data_lists)} linhas concluída com sucesso para {target_table}.")

    
    # DEFINE O FLUXO: Apenas uma tarefa
    full_load_situacao(ETL_CONFIG_SITUACAO)


# Instancia o DAG
single_full_load_situacao_dag()