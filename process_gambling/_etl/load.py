import boto3
import sqlite3
import pandas as pd

from process_gambling._etl.extract import Extract


class Load(Extract):

    def load(self, df: pd.DataFrame, table_name: str):
        conn = self.connect_to_db()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        self.close_db(conn)

    def sync_from_s3(self):
        pass

    @staticmethod
    def save_to_s3(filename: str, object_name: str):
        client = boto3.client('s3')
        client.upload_file(filename, 'scott-p-pwhite', object_name)

