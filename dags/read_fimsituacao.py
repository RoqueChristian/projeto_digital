from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pendulum
import pandas as pd

@dag(
    dag_id="read_dim_situacao",
    start_date=pendulum.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "extract"],
)
def read_dim_situacao_dag():

    @task
    def read_table():
        hook = MsSqlHook(mssql_conn_id="mssql_target")
        df = hook.get_pandas_df("""
            SELECT 
                ID_Situacao,
                ID_Situacao_Origem,
                Descricao_Situacao
            FROM DimSituacao
        """)
        print("✅ DimSituacao carregada com sucesso!")
        print(df.head())
        return df.to_json()

    read_table()

read_dim_situacao_dag()
