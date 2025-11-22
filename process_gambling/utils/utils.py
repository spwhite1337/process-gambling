import os
import sqlite3
import pandas as pd

from process_gambling import DATA_VERSION


def run_query(query: str) -> pd.DataFrame
    if os.environ.get('DB_ENGINE', 'SQLITE') == 'SQLITE':
        conn = sqlite3.connect(f'{os.getcwd()}/cache/process_gambling_{DATA_VERSION}.db')
        df = pd.read_sql(query)
        conn.close()
        return df
    else:
        raise NotImplementedError()

