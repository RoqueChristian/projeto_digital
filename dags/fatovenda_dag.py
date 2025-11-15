from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd
import numpy as np 
import sys # Para melhor log de erros

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="etl_fatovenda_pg_to_mssql_full_refresh",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "venda", "full_refresh"],
)
def etl_fato_venda_full_refresh():
    
    # Função auxiliar para Lookup (mantida)
    def get_lookup_dict(mssql, table_name, business_key, surrogate_key):
        df_lookup = mssql.get_pandas_df(f"SELECT {business_key}, {surrogate_key} FROM {table_name};")
        if business_key == 'data':
             df_lookup[business_key] = df_lookup[business_key].astype(str)
        return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

    # ✅ 1. EXTRAÇÃO — PostgreSQL (Full Load)
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT 
                nf.id_vendedor, 
                nf.id_cliente, 
                nf.id_forma_pagto, 
                CAST(nf.data_venda AS DATE) AS data_venda, 
                nf.numero_nf, 
                nf.valor AS valor_total_nf, 
                inf.id_produto, 
                inf.quantidade, 
                inf.valor_unitario, 
                inf.valor_venda_real,
                nf.id AS id_nota_fiscal 
            FROM 
                vendas.nota_fiscal nf
            LEFT JOIN 
                vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id;
        """
        df = pg.get_pandas_df(sql)
        print("✅ Extraído do PostgreSQL. Registros:", len(df))
        return df.to_dict(orient="records")

    # ✅ 2. TRANSFORMAÇÃO & LOOKUP
    @task
    def transform_and_lookup(rows):
        if not rows:
            print("⚠️ Nenhum dado encontrado.")
            return []

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        df = pd.DataFrame(rows)
        
        # Carregar Lookups
        lookup_vendedor = get_lookup_dict(mssql, "DimVendedor", "id_vendedor", "id_dim_vendedor")
        lookup_cliente = get_lookup_dict(mssql, "DimCliente", "id_cliente", "id_dim_cliente")
        lookup_formapagto = get_lookup_dict(mssql, "DimFormaPagamento", "id_forma_pagamento", "id_dim_forma_pagamento")
        lookup_produto = get_lookup_dict(mssql, "DimProduto", "id_produto", "id_dim_produto")
        lookup_tempo = get_lookup_dict(mssql, "DimTempo", "data", "id_dim_tempo")

        print("➡️ Aplicando Lookups para Chaves Substitutas...")
        
        # Mapeamentos
        df['data_venda_str'] = df['data_venda'].astype(str)
        
        df['id_dim_vendedor'] = df['id_vendedor'].map(lookup_vendedor).fillna(-1).astype(int)
        df['id_dim_cliente'] = df['id_cliente'].map(lookup_cliente).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagto'].map(lookup_formapagto).fillna(-1).astype(int)
        df['id_dim_produto'] = df['id_produto'].map(lookup_produto).fillna(-1).astype(int)
        df['id_dim_tempo_venda'] = df['data_venda_str'].map(lookup_tempo).fillna(-1).astype(int)
        
        # Filtrar colunas finais e registros com chaves faltantes
        df_final = df[[
            'id_dim_produto', 'id_dim_cliente', 'id_dim_vendedor', 'id_dim_forma_pagamento', 'id_dim_tempo_venda',
            'numero_nf', 'valor_total_nf', 'quantidade', 'valor_unitario', 'valor_venda_real'
        ]].copy()
        
        df_final = df_final[
            (df_final['id_dim_produto'] != -1) & 
            (df_final['id_dim_cliente'] != -1) & 
            (df_final['id_dim_tempo_venda'] != -1)
        ]
        
        print(f"✅ Registros prontos para carga (após lookup): {len(df_final)}")
        return df_final.to_dict(orient="records")

    # ✅ 3. LOAD — Gravar no SQL Server (Truncate & Insert)
    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        
        num_rows = len(rows)
        print(f"➡️ Estratégia Full Refresh: Deletando dados existentes e inserindo {num_rows} novos registros na FatoVenda.")
        
        # 1. TRUNCATE TABLE
        try:
            cursor.execute("TRUNCATE TABLE FatoVenda;")
            print("✅ Tabela FatoVenda truncada com sucesso.")
        except Exception as e:
            # Em tabelas Fato (que não costumam ter FKs para outras Fato), TRUNCATE é seguro.
            # No entanto, ele pode falhar se houver restrições específicas ou log.
            conn.rollback()
            print(f"❌ ERRO FATAL ao truncar FatoVenda: {e}", file=sys.stderr)
            raise e

        # 2. INSERT: 10 colunas (sem a PK IDENTITY)
        insert_sql = """
            INSERT INTO FatoVenda (
                id_dim_produto, id_dim_cliente, id_dim_vendedor, id_dim_forma_pagamento, id_dim_tempo_venda,
                numero_nf, valor_total_nf, quantidade, valor_unitario, valor_venda_real
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepara os dados na ordem correta
        data_to_insert = [
            (
                row["id_dim_produto"],
                row["id_dim_cliente"],
                row["id_dim_vendedor"],
                row["id_dim_forma_pagamento"],
                row["id_dim_tempo_venda"],
                row["numero_nf"],
                row["valor_total_nf"],
                row["quantidade"],
                row["valor_unitario"],
                row["valor_venda_real"],
            ) for row in rows
        ]

        # 3. Execução da Inserção
        try:
            cursor.executemany(insert_sql, data_to_insert)
            conn.commit()
            rows_affected = cursor.rowcount
            print(f"✅ {rows_affected} registros inseridos com sucesso na FatoVenda.")
            
            return f"{rows_affected} registros carregados via Full Refresh."
            
        except Exception as e:
            conn.rollback()
            print(f"❌ ERRO durante a inserção na FatoVenda: {e}", file=sys.stderr)
            raise e

    # ORQUESTRAÇÃO
    raw_data = extract_postgres()
    treated_data = transform_and_lookup(raw_data)
    load_mssql(treated_data)


etl_fato_venda_full_refresh()