import logging
from typing import Any
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_file_to_s3(aws_conn_id: str, bucket_name: str, key: str, data: Any) -> None:
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
    logging.info(f'File uploaded to S3: {bucket_name}/{key}')


def download_file_from_s3(aws_conn_id: str, bucket_name: str, key: str) -> bytes:
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    logging.info(f'File downloaded from S3: {bucket_name}/{key}')
    return obj['Body'].read()
