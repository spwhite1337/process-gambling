from typing import Tuple
import pandas as pd

from process_gambling.model.etl import Etl



class Wrangle(Etl):

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Interaction terms
        for t in self.interactions:
            df[t[0] + '_x_' + t[1]] = df[t[0]] * df[t[1]]

        df = df[self.GROUP_COLS + self.features + [self.response_col]]
        # Drop na
        df = df.dropna()
        return df

    def fit_transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Maybe drop 2020 and prior due to different game? Maybe time filter?
        # df = df[df['season'] > 2020]
        df = self._transform(df)
        df_train = df[df['season'] < self.model_params['val_year']].copy()
        df_test = df[df['season'] == self.model_params['val_year']].copy()
        return df_train, df_test

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._transform(df)
        return df

