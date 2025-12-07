import os
import boto3
import sqlite3
import pandas as pd

from sklearn.model_selection import BaseCrossValidator

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



class WalkForwardCV(BaseCrossValidator):
    def __init__(self, n_splits: int, n_train: int):
        """
        Assumes data is sorted before passed to self.split
        
        n_splits: number of folds
        train_size: fixed number of training samples (None = expanding window)
        test_size: number of samples in each test fold
        step_size: how far to move the window each split
        
        n_samples = n_train + n_splits * n_test
        n_test = (n_samples - n_train) // n_splits
        """
        self.n_splits = n_splits
        self.n_train = n_train
        self.n_test = None

    def split(self, X, y=None, groups=None):
        n_samples = len(X)
        self.n_test = (n_samples - self.n_train) // self.n_splits

        for i in range(self.n_splits):
            train_start = i * self.n_test
            train_end = train_start + self.n_train

            test_start = train_end
            test_end = test_start + self.n_test

            if test_end > n_samples:
                break

            train_idx = np.arange(0, train_end)
            test_idx = np.arange(test_start, test_end)

            yield train_idx, test_idx

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_splits

