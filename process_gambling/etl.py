import os
import boto3
import pandas as pd

from process_gambling._etl import Etl
from process_gambling.utils.utils import _data_exists_in_s3, run_query
from process_gambling.utils.queries import queries
from process_gambling import DATA_VERSION


class Run(Etl):

    @staticmethod
    def download_data_from_s3() -> bool:
        if _data_exists_in_s3():
            # Check if database exists in s3
            cache_dir = os.path.join(os.getcwd(), 'cache')
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            client = boto3.client('s3')
            client.download_file(
                'scott-p-white',
                f'code/process_gambling/data/process_gambling_{DATA_VERSION}.db',
                os.path.join(cache_dir, f'process_gambling_{DATA_VERSION}.db')
            )
            print(f'Downloaded Data Version {DATA_VERSION}')
            return True
        else:
            return False

    def transform(self):
        self.transform_events()
        self.transform_scores()
        self.transform_odds()

    def append_updates(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()
        tables_to_append = [
            'BRONZE_SCORES_{self.scores_data_source}_{self.sport}',
            'BRONZE_ODDSAPI_EVENTS_{self.sport}',
            'BRONZE_ODDSAPI_HIST_ODDS_{self.sport}',
            'SILVER_EVENT_ODDS_{self.sport}',
            'SILVER_EVENT_SCORES_{self.sport}',
            'SILVER_EVENTS_LOOKUP_{self.sport}'
        ]
        for table_to_append in tables_to_append:
            # Run append command
            cursor.execute(f"""
                INSERT INTO {table_to_append} 
                SELECT *
                FROM {table_to_append}_UPDATE;
            """)
            # Drop update table
            cursor.execute(f'DROP TABLE {table_to_append}_UPDATE;')
        self.close_db(conn)
        return

    def curate(self):
        query = queries[self.sport]['curate'][self.pull_type]
        df = run_query(query)
        self.upload(df, f'GOLD_CURATE_TEAM_EVENTS_{DATA_VERSION}')
        
        # Rolling windows and aggregate to EVENTS
        n_gamess, metrics = [3, 5, 7], [
            'team_win',
            'team_win_ats', 
            'team_margin_ats_abs', 
            'over_win', 
            'over_margin_abs',
            'team_margin_ats_3',
            'team_margin_ats_4',
            'team_margin_ats_7',
            'team_margin_ats_10',
        ]
        df_tmp = []
        for team_name, df__ in df.groupby('team'):
            for n_games in n_gamess:
                for metric in metrics:
                    df__[f'{metric}_window_{n_games}'] = df__[metric].shift().rolling(n_games).mean()
            df_tmp.append(df__)
        df_in = pd.concat(df_tmp)

        # Get one record for an event, defined as the home-team
        df = df_in[df_in['is_home'] == 1]
        df_opp = df_in[df_in['is_home'] == 0].\
            drop('opponent', axis=1).rename(columns={'team_name': 'opponent'})
        subset_cols = []
        for n_games in n_gamess:
            for metric in metrics:
                col = f'{metric}_window_{n_games}'
                df_opp = df_opp.rename(columns={col: 'opponent_' + col})
                subset_cols.append('opponent_' + col)
        df = df.merge(df_opp[['event_id', 'opponent'] + subset_cols], on=['event_id', 'opponent'])
        self.upload(df, f'GOLD_CURATE_EVENTS_{DATA_VERSION}')
        return df
    
    def _run(self):
        if self.pull_type == 'initial':
            df = self.extract_sports()
            self.upload(df, f'BRONZE_ODDSAPI_SPORTS')

            df = self.extract_participants()
            self.upload(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{self.sport}')

            df = self.generate_participants_lookup()
            self.upload(df, f'SILVER_TEAM_LOOKUPS_{self.sport}')

        df = self.extract_scores()
        print(f'Downloaded {df.shape[0]} Rows for Scores')
        self.upload(df, f'BRONZE_SCORES_{self.scores_data_source}_{self.sport}{self.table_appendix}')

        event_starts = self.download_event_starts()
        print(f'Downloaded {len(event_starts)} Event Starts')
        df = self.extract_events(event_starts)
        print(f'Downloaded {df.shape[0]} Rows for Events')
        self.upload(df, f'BRONZE_ODDSAPI_EVENTS_{self.sport}{self.table_appendix}')

        df_events = self.download(f'BRONZE_ODDSAPI_EVENTS_{self.sport}{self.table_appendix}')
        print(f'Downloaded {df_events.shape[0]} Rows for ODDS-Events')
        df = self.extract_odds(df_events)
        print(f'Downloaded {df.shape[0]} Rows for ODDS')
        self.upload(df, f'BRONZE_ODDSAPI_HIST_ODDS_{self.sport}{self.table_appendix}')

        self.transform()
        
        # Before curate, append UPDATE tables
        if self.pull_type == 'update':
            self.append_updates()

        self.curate()


    def run(self):
        # self.curate()
        # if True:
        #     return
        if self.pull_type == 'initial':
            if not self.download_data_from_s3():
                self._run()
        elif self.pull_type == 'update':
            self._run()
        else:
            raise NotImplementedError()


def run(sport: str, pull_type: str = 'initial', archive: bool = False):
    api = Run(sport=sport, pull_type=pull_type)
    api.run()

    if archive:
        api.save_to_s3(
                os.path.join(os.getcwd(), 'cache', f'process_gambling_{DATA_VERSION}.db'),
                f'code/process_gambling/data/process_gambling_{DATA_VERSION}.db'
            )


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    parser.add_argument('--pull_type', type=str)
    parser.add_argument('--archive', action='store_true')
    args = parser.parse_args()
    run(sport=args.sport, pull_type=args.pull_type, archive=args.archive)

