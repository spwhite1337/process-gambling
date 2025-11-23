from typing import Tuple
import pandas as pd

from process_gambling.model.etl import Etl



class Wrangle(Etl):

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop na
        df = df.dropna()
        return df

    def fit_transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df[self.GROUP_COLS + self.features + [self.response_col]]
        # Maybe drop 2020 and prior due to different game? Maybe time filter?
        # df_all = df_all[df_all['season'] > 2020]

        df_train = df[df['season'] < self.model_params['val_year']].copy()
        df_test = df[df['season'] == self.model_params['val_year']].copy()
        return df_train, df_test

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._transform(df)
        return df

