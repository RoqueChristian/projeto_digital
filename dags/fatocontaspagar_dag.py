from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd
import numpy as np
import os
import sys

# Configurações de Conexão e Staging
POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

# Definição dos Staging Paths específicos para Contas a Pagar
RAW_STAGING_PATH_CP = "/tmp/contas_pagar_raw.csv" 
TRANSFORMED_STAGING_PATH_CP = "/tmp/contas_pagar_transformed.csv"

@dag(
    dag_id="etl_fatocontaspagar_pg_to_mssql", 
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "contas_pagar", "simples"],
)
def etl_fato_contas_pagar_v2_simple():
    
    def get_lookup_dict(mssql_hook, table_name, business_key, surrogate_key):
        """Função auxiliar para obter dicionários de lookup de Dimensões."""
        df_lookup = mssql_hook.get_pandas_df(f"SELECT {business_key}, {surrogate_key} FROM {table_name};")
        if df_lookup.empty:
            print(f"Aviso: Tabela de Dimensão {table_name} está vazia.")
            return {}
        
        if business_key == 'data':
             df_lookup[business_key] = df_lookup[business_key].astype(str)
        
        return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

# --------------------------------------------------------------------------------------
## 1. Extração: PostgreSQL (Consulta Simples)
    @task
    def extract_postgres_to_file():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT
                cp.id AS id_conta_pagar,
                cp.id_situacao,
                cp.valor_original,
                cp.valor_atual,
                cp.emissao AS data_emissao,        
                cp.vencimento AS data_vencimento, 
                cp.data_pagamento,
                cp.documento,                      
                cp.id_forma_pagamento,
                cp.descricao AS descricao_pagamento 
            FROM financeiro.conta_pagar cp;
        """
        print("✅ Extraindo FatoContasAPagar (Simples) do PostgreSQL.")
        df = pg.get_pandas_df(sql)
        
        if df.empty:
            raise Exception("A extração do PostgreSQL não retornou dados. Terminando o pipeline.")
        
        df.to_csv(RAW_STAGING_PATH_CP, index=False)
        print(f"✅ Dados RAW salvos em: {RAW_STAGING_PATH_CP}")
        
        return RAW_STAGING_PATH_CP

# --------------------------------------------------------------------------------------
## 2. Transformação & Lookup
    @task
    def transform_and_lookup(raw_staging_path):
        if not os.path.exists(raw_staging_path):
             raise FileNotFoundError(f"Arquivo RAW de Staging não encontrado em {raw_staging_path}")
             
        df = pd.read_csv(raw_staging_path)
        print(f"➡️ Carregados {len(df)} registros do Staging File RAW para transformação.")
        
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        # Lookups necessários
        lookup_situacao = get_lookup_dict(mssql, "DimSituacaoTitulo", "id_situacao", "id_dim_situacao")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_forma_pagamento", "id_dim_forma_pagamento")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        # Mapeamento de Chaves Substitutas
        df['data_vencimento_str'] = df['data_vencimento'].astype(str)
        df['data_pagamento_str'] = df['data_pagamento'].astype(str).replace({'None': np.nan}) 
        df['data_emissao_str'] = df['data_emissao'].astype(str) 

        df['id_dim_situacao'] = df['id_situacao'].map(lookup_situacao).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagamento'].map(lookup_formapagto).fillna(-1).astype(int)
        
        df['id_dim_tempo_vencimento'] = df['data_vencimento_str'].map(lookup_tempo).fillna(-1).astype(int)
        df['id_dim_tempo_pagamento'] = df['data_pagamento_str'].map(lookup_tempo).fillna(np.nan) 
        df['id_dim_tempo_emissao'] = df['data_emissao_str'].map(lookup_tempo).fillna(-1).astype(int) 
        
        # Seleção de colunas finais
        df_final = df[[
            'id_conta_pagar', 
            'id_dim_situacao', 'id_dim_forma_pagamento',
            'id_dim_tempo_emissao', 'id_dim_tempo_vencimento', 'id_dim_tempo_pagamento',
            'valor_original', 'valor_atual', 'documento', 'descricao_pagamento'
        ]].copy()
        
        # Filtro de integridade
        df_final = df_final[
            (df_final['id_dim_situacao'] != -1) & 
            (df_final['id_dim_tempo_vencimento'] != -1) &
            (df_final['id_dim_tempo_emissao'] != -1)
        ]
        
        df_final.to_csv(TRANSFORMED_STAGING_PATH_CP, index=False)
        print(f"✅ Transformação concluída. Salvando em: {TRANSFORMED_STAGING_PATH_CP}")
        
        return TRANSFORMED_STAGING_PATH_CP

# --------------------------------------------------------------------------------------
## 3. Load: SQL Server
    @task
    def load_mssql(transformed_staging_path):
        
        def clean_staging_files():
            if os.path.exists(RAW_STAGING_PATH_CP): os.remove(RAW_STAGING_PATH_CP)
            if os.path.exists(transformed_staging_path): os.remove(transformed_staging_path)

        if not os.path.exists(transformed_staging_path):
             clean_staging_files()
             raise FileNotFoundError(f"Arquivo TRANSFORMADO de Staging não encontrado em {transformed_staging_path}")
             
        df_final = pd.read_csv(transformed_staging_path)
        rows_to_insert = df_final.to_dict(orient="records")
        
        if not rows_to_insert: 
            clean_staging_files()
            return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # 1. Idempotência
        loaded_ids = []
        try:
             df_loaded = mssql.get_pandas_df("SELECT id_conta_pagar FROM FatoContasAPagar;")
             if not df_loaded.empty:
                 loaded_ids = df_loaded['id_conta_pagar'].astype(np.int64).tolist()
        except Exception as e:
             print(f"Aviso: Falha ao consultar IDs carregados. Erro: {e}", file=sys.stderr)
             
        # 2. Filtrar
        rows_to_insert_final = []
        for row in rows_to_insert:
            try:
                current_id = int(row['id_conta_pagar']) 
                if current_id not in loaded_ids:
                    rows_to_insert_final.append(row)
            except ValueError:
                print(f"AVISO: Pulando registro com ID inválido: {row['id_conta_pagar']}")
                continue
        
        if not rows_to_insert_final: 
            clean_staging_files()
            return "Todos os registros já existem na FatoContasAPagar."

        print(f"✅ Inserindo {len(rows_to_insert_final)} novos registros na FatoContasAPagar...")
        
        # 3. Definição do INSERT (10 COLUNAS, sem a PK IDENTITY)
        insert_sql = """
            INSERT INTO FatoContasAPagar (
                id_dim_situacao, id_dim_forma_pagamento, 
                id_dim_tempo_emissao, id_dim_tempo_vencimento, id_dim_tempo_pagamento,         
                id_conta_pagar, valor_original, valor_atual, 
                documento, descricao_pagamento             
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """ 

        data_to_insert = []
        for row in rows_to_insert_final:
            id_pagamento = row["id_dim_tempo_pagamento"]
            if pd.isna(id_pagamento): id_pagamento = None
            # NÓS VAMOS PASSAR None para o campo INT no SQL Server.
            else: id_pagamento = int(id_pagamento)

            data_to_insert.append((
                int(row["id_dim_situacao"]),
                int(row["id_dim_forma_pagamento"]),
                int(row["id_dim_tempo_emissao"]),
                int(row["id_dim_tempo_vencimento"]),
                id_pagamento,                 
                int(row["id_conta_pagar"]), 
                row["valor_original"],
                row["valor_atual"],
                row["documento"],
                row["descricao_pagamento"],
            ))

        # 4. Execução (Voltando ao executemany/execute mais seguro)
        rows_affected = 0
        try:
             # Retornamos ao método de loop 'execute' por ser o mais estável 
             # contra o bug de IDENTITY/NULL do pymssql, mesmo com a correção da tabela.
            for row_data in data_to_insert:
                cursor.execute(insert_sql, row_data)
                rows_affected += 1
            
            conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"ERRO DE INSERÇÃO NO SQL SERVER: {e}", file=sys.stderr)
            raise e 

        # 5. Limpeza final
        clean_staging_files()
        print(f"✅ Arquivos de Staging removidos. {rows_affected} registros carregados.")
        
        return f"{rows_affected} registros carregados na FatoContasAPagar."


    # --------------------------------------------------------------------------------------
    # ORQUESTRAÇÃO CORRIGIDA COM ENCADEMENTO EXPLÍCITO
    
    task_extract = extract_postgres_to_file()
    task_transform = transform_and_lookup(task_extract)
    task_load = load_mssql(task_transform)
    
    task_extract >> task_transform >> task_load
    
    
etl_fato_contas_pagar_v2_simple()