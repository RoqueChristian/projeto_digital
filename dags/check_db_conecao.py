from __future__ import annotations
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"


@dag(
    dag_id="01_check_db_connections",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["healthcheck", "connection"],
)
def check_db_connections_dag():

    @task
    def check_postgres_source_connection():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        result = hook.get_first("SELECT version();")
        print("✅ PostgreSQL conectado:", result)
        return "Postgres OK"

    @task
    def check_mssql_target_connection():
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, @@VERSION;")
        result = cursor.fetchone()
        print("✅ SQL Server conectado:", result)
        return "SQL Server OK"

    check_postgres_source_connection()
    check_mssql_target_connection()


check_db_connections_dag()
