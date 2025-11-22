import pandas as pd

from process_gambling.model.params import Params
from process_gambling.utils.utils import run_query
from process_gambling.utils.queries import queries


class Etl(Params):

    def download(self) -> pd.DataFrame: 
        query = queries[self.sport]['training']
        df = run_query(query)
        return df

