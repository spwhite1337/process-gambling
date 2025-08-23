from typing import List
import boto3
import sqlite3
import pandas as pd

from process_gambling._etl.extract import Extract
from process_gambling._etl.helpers import ExtractionHelpersOddsApi
from process_gambling.config import logger


class Load(Extract, ExtractionHelpersOddsApi):

    def upload(self, df: pd.DataFrame, table_name: str):
        conn = self.connect_to_db()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        self.close_db(conn)

    def download(self, table_name: str) -> pd.DataFrame:
        conn = self.connect_to_db()
        df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
        self.close_db(conn)
        return df

    def download_event_starts(self) -> List[str]:
        # Get event-starts for parameters in the ODDS_API
        if self.sport == 'americanfootball_nfl':
            conn = self.connect_to_db()
            logger.info(f'Downloading Event-Starts for {self.sport}')
            df = pd.read_sql(f"""
                SELECT DISTINCT kickoff_datetime
                FROM BRONZE_SCORES_{self.scores_data_source}_{self.sport}
                -- Historical ODDS_API data starts at June 6, 2020
                WHERE kickoff_datetime > DATE('2020-06-06')
                -- Impute some dates manually that didn't align between systems
                {self.manual_impute_event_starts}
                ORDER BY kickoff_datetime
                """, conn)
            self.close_db(conn)
            event_starts = df['kickoff_datetime'].to_list()
        else:
            event_starts = []
        return event_starts

    def sync_from_s3(self):
        pass

    @staticmethod
    def save_to_s3(filename: str, object_name: str):
        client = boto3.client('s3')
        client.upload_file(filename, 'scott-p-white', object_name)

