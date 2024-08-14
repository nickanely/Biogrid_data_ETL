import logging
from io import BytesIO
from typing import Any, Dict

import pandas as pd
from airflow.models import Variable


from biogrid_ETL.utils.db_operations import get_postgres_engine, ingest_data_to_db
from biogrid_ETL.utils.s3_operations import download_file_from_s3

AWS_CONN_ID = 'aws_default'
POSTGRES_CONN_ID = 'postgres_local'
S3_BUCKET = Variable.get('s3_bucket')
S3_KEY_PREFIX = Variable.get('s3_key_prefix')
BIOGRID_URL = Variable.get('biogrid_url')
TABLE_NAME = 'biogrid_data_new'


def ingest_data(params: Dict[str, Any], ti: Any) -> None:
    biogrid_version = params['version']
    s3_key = ti.xcom_pull(task_ids='load_data', key='s3_key')

    file_content = download_file_from_s3(AWS_CONN_ID, S3_BUCKET, s3_key)
    df = pd.read_csv(BytesIO(file_content), delimiter='\t', compression='zip', nrows=100)

    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )
    df = df[[
        'biogrid_interaction_id',
        'biogrid_id_interactor_a',
        'biogrid_id_interactor_b',
    ]]
    df['version'] = biogrid_version

    logging.info('Biogrid file has been transformed')
    logging.info('Starting ingestion into database...')

    engine = get_postgres_engine(POSTGRES_CONN_ID)
    ingest_data_to_db(engine, df, TABLE_NAME)
