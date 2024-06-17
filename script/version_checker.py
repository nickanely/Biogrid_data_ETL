from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Any, Dict

POSTGRES_CONN_ID = 'postgres_local'


def check_version_existence(params: Dict[str, Any]) -> str:
    biogrid_version = params['version']
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:
        cursor.execute('SELECT DISTINCT(bd."version") FROM biogrid_data_new bd;')
        loaded_versions = [version[0] for version in cursor.fetchall()]

    if biogrid_version not in loaded_versions:
        return 'load_data'
    else:
        return 'finish'
