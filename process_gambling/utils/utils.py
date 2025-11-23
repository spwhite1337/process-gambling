import os
import boto3
import sqlite3
import pandas as pd

from process_gambling import DATA_VERSION


def _data_exists_in_s3() -> bool:
    client = boto3.client('s3')
    try:
        client.head_object(
                Bucket='scott-p-white',
                Key=f'code/process_gambling/data/process_gambling_{DATA_VERSION}.db'
             )
        return True
    except:
        return False


def run_query(query: str) -> pd.DataFrame:
    if os.environ.get('DB_ENGINE', 'SQLITE') == 'SQLITE':
        if not os.path.exists(f'{os.getcwd()}/cache/process_gambling_{DATA_VERSION}.db'):
            raise FileNotFoundError('No .db locally, did you download it from S3?')
        conn = sqlite3.connect(f'{os.getcwd()}/cache/process_gambling_{DATA_VERSION}.db')
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    else:
        raise NotImplementedError()

