import logging
from typing import Any, Dict

import requests


from biogrid_ETL.utils.s3_operations import upload_file_to_s3

from airflow.models import Variable

AWS_CONN_ID = 'aws_default'
POSTGRES_CONN_ID = 'postgres_local'
S3_BUCKET = Variable.get('s3_bucket')
S3_KEY_PREFIX = Variable.get('s3_key_prefix')
BIOGRID_URL = Variable.get('biogrid_url')
TABLE_NAME = 'biogrid_data_new'


def load_biogrid(params: Dict[str, Any], ti: Any) -> None:
    biogrid_version = params['version']
    local_file_name = f'biogrid_v{biogrid_version.replace(".", "_")}.tab3.zip'
    s3_key = f'{S3_KEY_PREFIX}/{local_file_name}'
    ti.xcom_push(
        key='s3_key',
        value=s3_key
    )

    logging.info('Loading biogrid file...')
    response = requests.get(
        BIOGRID_URL.format(version=biogrid_version),
        params={'downloadformat': 'zip'}
    )

    if response.status_code == 200:
        upload_file_to_s3(AWS_CONN_ID, S3_BUCKET, s3_key, response.content)
    else:
        logging.error('The specified version is not found')
        raise Exception('The specified version is not found')

    logging.info('Biogrid file has loaded and uploaded to S3')

