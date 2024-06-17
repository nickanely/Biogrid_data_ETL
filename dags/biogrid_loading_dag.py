import pendulum

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from biogrid_ETL.scripts.ingest_data import ingest_data
from biogrid_ETL.scripts.load_biogrid import load_biogrid
from biogrid_ETL.scripts.get_latest_version import get_latest_version
from biogrid_ETL.scripts.check_version_existence import check_version_existence


def determine_version(params):
    version = params['version']
    if version.lower() == 'latest':
        version = get_latest_version()
    return version


with DAG(
        dag_id='biogrid_loading_dag',
        start_date=pendulum.datetime(2024, 6, 18),
        schedule=None,
        tags=['biogrid', 'biogrid_HW', 'Homework_32.2'],
        description='A DAG to load biogrid from website into Postgres database',
        catchup=False,
        params={
            'version': Param('4.4.200', type='string')
        }
) as dag:
    start_op = EmptyOperator(task_id='start')

    determine_version_op = PythonOperator(
        task_id='determine_version',
        python_callable=determine_version,
    )

    check_version_existence_op = BranchPythonOperator(
        task_id='check_version_existence',
        python_callable=check_version_existence
    )

    load_data_op = PythonOperator(
        task_id='load_data',
        python_callable=load_biogrid
    )

    ingest_data_op = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    trigger_function_op = PostgresOperator(
        task_id='trigger_function',
        sql='SELECT get_biogrid_interactors();',
        postgres_conn_id='postgres_local'
    )

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

start_op >> load_data_op >> ingest_data_op >> trigger_function_op >> finish_op
start_op >> check_version_existence_op >> finish_op
