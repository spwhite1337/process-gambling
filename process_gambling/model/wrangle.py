from typing import Tuple
import pandas as pd

from process_gambling.model.etl import Etl



class Wrangle(Etl):

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Avoid setting-with-copy warning
        df = df.copy()
        # Interaction terms
        for t in self.interactions:
            x_col = t[0] + '_x_' + t[1]
            if x_col in self.features:
                df[x_col] = df[t[0]] * df[t[1]]

        df = df[self.GROUP_COLS + self.features + [self.response_col]]
        # Drop na
        df = df.dropna()
        return df

    def fit_transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # Maybe drop 2020 and prior due to different game? Maybe time filter?
        df = df[df['season'] > 2020]
        df = self._transform(df)
        df = df.sort_values(['event_start', 'event_id'], ascending=True).reset_index(drop=True)

        df_train = df[~df['event_id'].isin(df['event_id'].tail(self.n_test))].copy()
        df_test = df[df['event_id'].isin(df['event_id'].tail(self.n_test))].copy()
        return df_train, df_test

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._transform(df)
        return df

