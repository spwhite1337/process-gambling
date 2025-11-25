import os
import boto3
import pickle
import pandas as pd

from process_gambling.model.params import Params
from process_gambling.etl import run as run_etl
from process_gambling.utils.utils import run_query
from process_gambling.utils.queries import queries
from process_gambling import DATA_VERSION, MODEL_VERSION, BUCKET_NAME


class Etl(Params):

    def _extract(self) -> pd.DataFrame: 
        query = f'SELECT * FROM GOLD_CURATE_EVENTS_{DATA_VERSION}'
        df = run_query(query)
        return df

    def _extract_update(self) -> pd.DataFrame: 
        query = f'SELECT * FROM GOLD_CURATE_EVENTS_UPDATE_{DATA_VERSION}'
        df = run_query(query)
        return df

    def download_train(self) -> pd.DataFrame:
        df = self._extract()
        return df

    def download_update(self) -> pd.DataFrame:
        df = self._extract_update()
        df = df[df['season'] == df['season'].max()]
        return df

    def save_model(self):
        model_fp = os.path.join(os.getcwd(), 'cache', f'model_{MODEL_VERSION}.pkl')
        if not os.path.exists(os.path.dirname(model_fp)):
            os.makedirs(os.path.dirname(model_fp))
        print(f'Saving Model: {MODEL_VERSION}')
        with open(model_fp, 'wb') as fp:
            pickle.dump(self, fp)

    @staticmethod
    def load_model():
        model_fp = os.path.join(os.getcwd(), 'cache', f'model_{MODEL_VERSION}.pkl')
        if not os.path.exists(model_fp):
            raise FileNotFoundError(model_fp)
        with open(model_fp, 'rb') as jp:
            out = pickle.load(jp)
        return out

    def download_model(self):
        if not boto3.client('s3').head_object(Bucket=BUCKET_NAME, Key=f'code/process_gambling/model/model_{MODEL_VERSION}.pkl'):
            raise FileNotFoundError(f'model_{MODEL_VERSION}.pkl')

        cache_dir = os.path.join(os.getcwd(), 'cache')
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        client = boto3.client('s3')
        client.download_file(
            BUCKET_NAME,
            f'code/process_gambling/model/model_{MODEL_VERSION}.pkl',
            os.path.join(cache_dir, f'model_{MODEL_VERSION}.pkl')
        )
        print(f'Downloaded Model: {MODEL_VERSION}')

    @staticmethod
    def upload_model():
        model_fp = os.path.join(os.getcwd(), 'cache', f'model_{MODEL_VERSION}.pkl')
        if not os.path.exists(model_fp):
            raise FileNotFoundError(model_fp)

        boto3.client('s3').\
                upload_file(
                    Filename=model_fp,
                    Bucket=BUCKET_NAME,
                    Key=f'code/process_gambling/model/model_{MODEL_VERSION}.pkl'
                )    
        print(f'Uploading Model {model_fp} to S3')

