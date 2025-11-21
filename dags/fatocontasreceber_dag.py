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

# Definição dos Staging Paths (Local Filesystem)
RAW_STAGING_PATH = "/tmp/contas_receber_raw.csv" 
TRANSFORMED_STAGING_PATH = "/tmp/contas_receber_transformed.csv"

@dag(
    dag_id="etl_fatocontasreceber_pg_to_mssql_incremental", # Nome ajustado
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "contas_receber", "incremental"],
)
def etl_fato_contas_receber_incremental():
    
    def get_lookup_dict(mssql_hook, table_name, business_key, surrogate_key):
        """Função auxiliar para obter dicionários de lookup de Dimensões."""
        df_lookup = mssql_hook.get_pandas_df(f"SELECT {business_key}, {surrogate_key} FROM {table_name};")
        if df_lookup.empty:
            print(f"Aviso: Tabela de Dimensão {table_name} está vazia.")
            return {}
        
        # Converte a chave de negócio para string se for DimTempo (data)
        if business_key == 'data':
             df_lookup[business_key] = df_lookup[business_key].astype(str)
        
        return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

# --------------------------------------------------------------------------------------
    # --- 1. EXTRAÇÃO: PostgreSQL (Full Load para garantir novos Fatos) ---
    @task
    def extract_postgres_to_file():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT
                cr.id AS id_conta_receber, cr.id_situacao, cr.id_forma_pagamento, nf.id_cliente, 
                cr.valor_original, cr.valor_atual, p.valor AS valor_parcela,
                CAST(cr.vencimento AS DATE) AS data_vencimento, 
                CAST(cr.data_recebimento AS DATE) AS data_recebimento,
                p.numero AS numero_parcela, nf.numero_nf
            FROM financeiro.conta_receber cr
            INNER JOIN vendas.parcela p ON p.id = cr.id_parcela
            INNER JOIN vendas.nota_fiscal nf ON nf.id = p.id_nota_fiscal; 
            -- ATENÇÃO: Nenhuma cláusula WHERE para filtro incremental baseado em tempo.
            -- O filtro será feito pelo Python/SQL Server, verificando a chave de negócio (id_conta_receber).
        """
        print("✅ Extraindo FatoContasAReceber (Full Load) do PostgreSQL e salvando no Staging File RAW.")
        df = pg.get_pandas_df(sql)
        
        if df.empty:
            print("A extração do PostgreSQL não retornou dados. Finalizando o pipeline.")
            return None # Retorna None para interromper o pipeline
        
        df.to_csv(RAW_STAGING_PATH, index=False)
        print(f"✅ Dados RAW salvos em: {RAW_STAGING_PATH}")
        
        return RAW_STAGING_PATH

# --------------------------------------------------------------------------------------
    # --- 2. TRANSFORMAÇÃO & LOOKUP ---
    @task
    def transform_and_lookup(raw_staging_path):
        if raw_staging_path is None: return None

        if not os.path.exists(raw_staging_path):
             raise FileNotFoundError(f"Arquivo RAW de Staging não encontrado em {raw_staging_path}")
             
        df = pd.read_csv(raw_staging_path)
        print(f"➡️ Carregados {len(df)} registros do Staging File RAW para transformação.")
        
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        
        # Lookups
        lookup_situacao = get_lookup_dict(mssql, "DimSituacaoTitulo", "id_situacao", "id_dim_situacao")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_forma_pagamento", "id_dim_forma_pagamento")
        lookup_cliente = get_lookup_dict(mssql, "DimCliente", "id_cliente", "id_dim_cliente")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        # Conversão e Mapeamento
        df['data_vencimento_str'] = df['data_vencimento'].astype(str)
        df['data_recebimento_str'] = df['data_recebimento'].astype(str).replace({'None': np.nan})
        
        df['id_dim_situacao'] = df['id_situacao'].map(lookup_situacao).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagamento'].map(lookup_formapagto).fillna(-1).astype(int)
        df['id_dim_cliente'] = df['id_cliente'].map(lookup_cliente).fillna(-1).astype(int)
        df['id_dim_tempo_vencimento'] = df['data_vencimento_str'].map(lookup_tempo).fillna(-1).astype(int)
        df['id_dim_tempo_recebimento'] = df['data_recebimento_str'].map(lookup_tempo).fillna(np.nan) 
        
        # Seleção de colunas finais e aplicação de filtros de integridade
        df_final = df[[
            'id_conta_receber', 
            'id_dim_situacao', 'id_dim_forma_pagamento', 'id_dim_cliente', 
            'id_dim_tempo_vencimento', 'id_dim_tempo_recebimento',
            'valor_parcela', 'valor_original', 'valor_atual', 'numero_parcela', 'numero_nf'
        ]].copy()
        
        df_final = df_final[
            (df_final['id_dim_situacao'] != -1) & 
            (df_final['id_dim_cliente'] != -1) & 
            (df_final['id_dim_tempo_vencimento'] != -1)
        ]
        
        df_final.to_csv(TRANSFORMED_STAGING_PATH, index=False)
        print(f"✅ Transformação concluída. Registros prontos: {len(df_final)}. Salvando em: {TRANSFORMED_STAGING_PATH}")
        
        return TRANSFORMED_STAGING_PATH

# --------------------------------------------------------------------------------------
    # --- 3. LOAD: SQL Server (Incremental & Idempotência) ---
    @task
    def load_mssql(transformed_staging_path):
        
        def clean_staging_files():
            """Função para limpar arquivos temporários."""
            if os.path.exists(RAW_STAGING_PATH): os.remove(RAW_STAGING_PATH)
            if os.path.exists(transformed_staging_path): os.remove(transformed_staging_path)

        if transformed_staging_path is None or not os.path.exists(transformed_staging_path):
             clean_staging_files()
             return "Carga interrompida: Nenhum dado novo encontrado ou arquivo não existe."
             
        df_final = pd.read_csv(transformed_staging_path)
        rows_to_insert = df_final.to_dict(orient="records")
        
        if not rows_to_insert: 
            clean_staging_files()
            return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        # 1. Obter IDs já carregados (para Idempotência)
        loaded_ids = []
        try:
             df_loaded = mssql.get_pandas_df("SELECT id_conta_receber FROM FatoContasAReceber;")
             if not df_loaded.empty:
                 loaded_ids = df_loaded['id_conta_receber'].astype(np.int64).tolist()
                 print(f"IDs de negócio já carregados: {len(loaded_ids)} registros.")
             else:
                 print("Tabela FatoContasAReceber vazia.")
        except Exception as e:
             # Este erro é esperado na primeira execução ou se a tabela estiver vazia
             print(f"Aviso: Falha ao consultar IDs carregados (ignorado). Erro: {e}", file=sys.stderr)
             loaded_ids = []
             
        # 2. Filtrar os registros que ainda não foram carregados
        rows_to_insert_final = []
        for row in rows_to_insert:
            try:
                current_id = int(row['id_conta_receber']) 
                if current_id not in loaded_ids:
                    rows_to_insert_final.append(row)
            except ValueError:
                print(f"AVISO: Pulando registro com ID inválido: {row['id_conta_receber']}")
                continue
        
        if not rows_to_insert_final: 
            clean_staging_files()
            return "Todos os registros já existem na FatoContasAReceber."

        print(f"✅ Inserindo {len(rows_to_insert_final)} novos registros na FatoContasAReceber...")
        
        # 3. Definição do INSERT (11 COLUNAS, 11 PLACEHOLDERS)
        insert_sql = """
            INSERT INTO FatoContasAReceber (
                id_dim_situacao, 
                id_dim_forma_pagamento, 
                id_dim_cliente, 
                id_dim_tempo_vencimento, 
                id_dim_tempo_recebimento,
                id_conta_receber, 
                valor_parcela, 
                valor_original, 
                valor_atual, 
                numero_parcela, 
                numero_nf
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """ 

        data_to_insert = []
        for row in rows_to_insert_final:
            id_recebimento = row["id_dim_tempo_recebimento"]
            # Trata NULL/NaN para colunas INT
            if pd.isna(id_recebimento):
                id_recebimento = None
            else:
                 id_recebimento = int(id_recebimento)

            # O tuple deve ter exatamente 11 valores, na ordem do INSERT
            data_to_insert.append((
                int(row["id_dim_situacao"]), 
                int(row["id_dim_forma_pagamento"]), 
                int(row["id_dim_cliente"]), 
                int(row["id_dim_tempo_vencimento"]), 
                id_recebimento, 
                int(row["id_conta_receber"]), 
                row["valor_parcela"], 
                row["valor_original"], 
                row["valor_atual"], 
                row["numero_parcela"], 
                row["numero_nf"], 
            ))

        # 4. Execução
        try:
            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()
            rows_affected = cursor.rowcount
        except Exception as e:
            conn.rollback()
            print(f"❌ ERRO durante a inserção na FatoContasAReceber: {e}", file=sys.stderr)
            raise e 

        # 5. Limpeza final
        clean_staging_files()
        print(f"✅ Arquivos de Staging removidos. {rows_affected} registros carregados.")
        
        return f"{rows_affected} registros carregados na FatoContasAReceber."


    # ORQUESTRAÇÃO
    raw_path = extract_postgres_to_file()
    transformed_path = transform_and_lookup(raw_path)
    load_mssql(transformed_path)


etl_fato_contas_receber_incremental()