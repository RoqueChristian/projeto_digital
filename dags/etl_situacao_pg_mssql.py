from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"


@dag(
    dag_id="etl_situacao_pg_to_mssql",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "dim", "situacao"],
)
def etl_situacao():

    # ✅ 1. EXTRAÇÃO — PostgreSQL
    @task
    def extract_postgres():
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sql = """
            SELECT 
                id AS ID_Situacao_Origem,
                descricao AS Descricao_Situacao
            FROM financeiro.situacao_titulo;
        """

        df = pg.get_pandas_df(sql)

        print("✅ Extraído do PostgreSQL:")
        print(df.head())
        print("✅ Colunas:", df.columns.tolist())

        return df.to_dict(orient="records")

    # ✅ 2. TRANSFORMAÇÃO — Tratamento e validação
    @task
    def transform_data(rows):
        if not rows:
            print("⚠️ Nenhum dado encontrado para transformar.")
            return []

        df = pd.DataFrame(rows)

        print("✅ Colunas recebidas:", df.columns.tolist())

        # Padronização de colunas (garantir nomes corretos)
        df = df.rename(
            columns={
                "id_situacao_origem": "ID_Situacao_Origem",
                "descricao_situacao": "Descricao_Situacao",
                "descricao": "Descricao_Situacao",
                "id": "ID_Situacao_Origem",
            }
        )

        # Remover espaços, normalizar texto
        df["Descricao_Situacao"] = (
            df["Descricao_Situacao"]
            .astype(str)
            .str.strip()
        )

        print("✅ Após transformação:")
        print(df.head())

        return df.to_dict(orient="records")

    # ✅ 3. LOAD — Gravar no SQL Server (apenas origem + descrição)
    @task
    def load_mssql(rows):
        if not rows:
            print("⚠️ Nada a carregar no SQL Server.")
            return "Zero registros."

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql.get_conn()
        cursor = conn.cursor()

        print(f"✅ Inserindo {len(rows)} registros no SQL Server...")

        insert_sql = """
            INSERT INTO DimSituacao (
                ID_Situacao_Origem,
                Descricao_Situacao
            )
            VALUES (%s, %s);
        """

        for row in rows:
            print("➡️ Inserindo:", row)
            cursor.execute(
                insert_sql,
                (
                    row["ID_Situacao_Origem"],
                    row["Descricao_Situacao"]
                ),
            )

        conn.commit()
        print("✅ Inserção concluída!")

        return f"{len(rows)} registros carregados."

    # ORQUESTRAÇÃO
    raw_data = extract_postgres()
    treated_data = transform_data(raw_data)
    load_mssql(treated_data)


etl_situacao()
