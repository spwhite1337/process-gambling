import os
import boto3
from process_gambling._etl import Etl
from process_gambling.utils.utils import _data_exists_in_s3
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
    
    def _run_initial(self):
        df = self.extract_sports()
        api.upload(df, f'BRONZE_ODDSAPI_SPORTS')

        df = api.extract_participants()
        api.upload(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{api.sport}')

        df = api.generate_participants_lookup()
        api.upload(df, f'SILVER_TEAM_LOOKUPS_{api.sport}')

        df = api.extract_scores()
        api.upload(df, f'BRONZE_SCORES_{api.scores_data_source}_{api.sport}')

        event_starts = api.download_event_starts()
        df = api.extract_events(event_starts)
        api.upload(df, f'BRONZE_ODDSAPI_EVENTS_{api.sport}')

        df_events = api.download(f'BRONZE_ODDSAPI_EVENTS_{api.sport}')
        df = api.extract_odds(df_events)
        api.upload(df, f'BRONZE_ODDSAPI_HIST_ODDS_{api.sport}')

        api.transform_events()
        api.transform_scores()
        api.transform_odds()


    def run(self):
        if self.pull_type == 'initial':
            if self.download_data_from_s3():
                return
            self._run_initial()


def run(sport: str, pull_type: str = 'initial'):
    api = Run(sport=sport, pull_type=pull_type)
    api.run()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    parser.add_argument('--pull_type', type=str)
    args = parser.parse_args()
    run(sport=args.sport)

