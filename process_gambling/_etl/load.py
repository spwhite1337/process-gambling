import sqlite3

from process_gambling._etl.extract import Extract


class Load(Extract):

    def sync_from_s3(self):
        pass

    def save_to_s3(self):
        pass

