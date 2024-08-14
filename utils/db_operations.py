import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_postgres_engine(postgres_conn_id: str) -> Engine:
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    connection = postgres_hook.get_conn()
    url = postgres_hook.get_uri()
    return create_engine(url)


def ingest_data_to_db(engine: Engine, df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, engine, if_exists='append', index=False)
    logging.info('Data successfully ingested into the database')
