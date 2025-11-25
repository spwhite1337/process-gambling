import os
import pandas as pd

from process_gambling.model.params import Params
from process_gambling.etl import run as run_etl
from process_gambling.utils.utils import run_query
from process_gambling.utils.queries import queries
from process_gambling import DATA_VERSION


class Etl(Params):

    def _extract(self) -> pd.DataFrame: 
        query = queries[self.sport]['training']
        df = run_query(query)
        return df

    def _transform_extraction(self, df: pd.DataFrame) -> pd.DataFrame:
        n_gamess, metrics = [3, 5, 7], [
            'team_win',
            'team_win_ats', 
            'team_margin_ats_abs', 
            'over_win', 
            'over_margin_abs'
        ]
        print(df.shape)
        dfs = []
        for team_name, df__ in df.groupby('team'):
            for n_games in n_gamess:
                for metric in metrics:
                    # Calculate rolling windows in pandas bc local sqlite is weird version
                    df__[f'{metric}_window_{n_games}'] = df__[metric].shift().rolling(n_games).mean()
            dfs.append(df__)
        df = pd.concat(dfs)
        print(df.shape)
        # Get one record for an event, defined as the home-team
        df_ = df[df['is_home'] == 1]
        df_opp = df[df['is_home'] == 0].\
            drop('opponent', axis=1).rename(columns={'team_name': 'opponent'})
        subset_cols = []
        for n_games in n_gamess:
            for metric in metrics:
                col = f'{metric}_window_{n_games}'
                df_opp = df_opp.rename(columns={col: 'opponent_' + col})
                subset_cols.append('opponent_' + col)
        df_ = df_.merge(df_opp[['event_id', 'opponent'] + subset_cols], on=['event_id', 'opponent'])
        return df_

    def download(self) -> pd.DataFrame:
        df = self._extract()
        df = self._transform_extraction(df)
        return df

