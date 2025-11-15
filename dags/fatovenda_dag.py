from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd
import numpy as np # Necessário para o tratamento de NaN/None no lookup

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

@dag(
    dag_id="etl_fatovenda_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "fato", "venda"],
)
def etl_fato_venda():
    
    # ✅ 1. EXTRAÇÃO — PostgreSQL
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
                nf.id AS id_nota_fiscal -- Adicionado para controle de registro (chave de negócio)
            FROM 
                vendas.nota_fiscal nf
            LEFT JOIN 
                vendas.item_nota_fiscal inf ON inf.id_nota_fiscal = nf.id;
        """

        df = pg.get_pandas_df(sql)
        print("✅ Extraído do PostgreSQL. Registros:", len(df))

        return df.to_dict(orient="records")

    # ✅ 2. TRANSFORMAÇÃO & LOOKUP — Substituição de Chaves
    @task
    def transform_and_lookup(rows):
        if not rows:
            print("⚠️ Nenhum dado encontrado.")
            return []

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        df = pd.DataFrame(rows)
        
        # 1. Carregar Lookups (Mapeamentos de Chave de Negócio -> Chave Substituta)
        
        def get_lookup_dict(table_name, business_key, surrogate_key):
            df_lookup = mssql.get_pandas_df(f"SELECT {business_key}, {surrogate_key} FROM {table_name};")
            # Converte a chave de negócio para string se for DimTempo (data)
            if business_key == 'data':
                 df_lookup[business_key] = df_lookup[business_key].astype(str)
            return pd.Series(df_lookup[surrogate_key].values, index=df_lookup[business_key]).to_dict()

        lookup_vendedor = get_lookup_dict("DimVendedor", "id_vendedor", "id_dim_vendedor")
        lookup_cliente = get_lookup_dict("DimCliente", "id_cliente", "id_dim_cliente")
        lookup_formapagto = get_lookup_dict("DimFormaPagamento", "id_forma_pagamento", "id_dim_forma_pagamento")
        lookup_produto = get_lookup_dict("DimProduto", "id_produto", "id_dim_produto")
        # Para DimTempo, a chave de negócio é a data
        lookup_tempo = get_lookup_dict("DimTempo", "data", "id_dim_tempo")

        # 2. Aplicar Mapeamentos (Lookup)
        print("➡️ Aplicando Lookups para Chaves Substitutas...")
        
        # Coluna de Data convertida para string 'YYYY-MM-DD' para o lookup do tempo
        df['data_venda_str'] = df['data_venda'].astype(str)
        
        # Mapeamento e tratamento de faltantes: usar -1 para "Missing"
        df['id_dim_vendedor'] = df['id_vendedor'].map(lookup_vendedor).fillna(-1).astype(int)
        df['id_dim_cliente'] = df['id_cliente'].map(lookup_cliente).fillna(-1).astype(int)
        df['id_dim_forma_pagamento'] = df['id_forma_pagto'].map(lookup_formapagto).fillna(-1).astype(int)
        df['id_dim_produto'] = df['id_produto'].map(lookup_produto).fillna(-1).astype(int)
        df['id_dim_tempo_venda'] = df['data_venda_str'].map(lookup_tempo).fillna(-1).astype(int)
        
        # 3. Filtrar colunas finais para a carga no FatoVenda
        # Utiliza o id_nota_fiscal e id_produto como chave de negócio combinada para garantir unicidade do item.
        df_final = df[[
            'id_nota_fiscal', # Chave de negócio para controle de inserção
            'id_dim_produto', 'id_dim_cliente', 'id_dim_vendedor', 'id_dim_forma_pagamento', 'id_dim_tempo_venda',
            'numero_nf', 'valor_total_nf', 'quantidade', 'valor_unitario', 'valor_venda_real'
        ]].copy()
        
        # 4. Filtrar registros com chaves faltantes (qualquer -1 na dimensão obrigatória)
        df_final = df_final[
            (df_final['id_dim_produto'] != -1) & 
            (df_final['id_dim_cliente'] != -1) & 
            (df_final['id_dim_tempo_venda'] != -1)
        ]
        
        print(f"✅ Registros prontos para carga (após lookup): {len(df_final)}")
        return df_final.to_dict(orient="records")

    # ✅ 3. LOAD — Gravar no SQL Server (Inserção Simples)
    @task
    def load_mssql(rows):
        if not rows: return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        
        # 1. Obter IDs já carregados (para evitar duplicação do item da NF)
        # Assumimos que a chave de negócio única para o item é: id_nota_fiscal + id_produto
        # No Fato, geralmente usamos uma chave composta se não houver um ID único no item.
        # Simplificando, podemos carregar todos os registros e confiar na PK ou fazer um cheque.
        
        # Neste caso, vamos apenas inserir, assumindo que esta carga é incremental ou a primeira
        # e a tabela FatoVenda permite duplicação de itens se o DAG for executado mais de uma vez
        # (O ideal seria usar uma chave de negócio no Fato ou um Staging/Merge para garantir idempotência).
        
        print(f"✅ Inserindo {len(rows)} registros na FatoVenda...")
        
        insert_sql = """
            INSERT INTO FatoVenda (
                id_dim_produto, id_dim_cliente, id_dim_vendedor, id_dim_forma_pagamento, id_dim_tempo_venda,
                numero_nf, valor_total_nf, quantidade, valor_unitario, valor_venda_real
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
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

        cursor.executemany(insert_sql, data_to_insert)

        conn.commit()
        print("✅ Inserção na FatoVenda concluída!")

        return f"{len(rows)} registros carregados."

    # ORQUESTRAÇÃO
    raw_data = extract_postgres()
    treated_data = transform_and_lookup(raw_data)
    load_mssql(treated_data)


etl_fato_venda()