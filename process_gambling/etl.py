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
        self.upload(df, f'BRONZE_ODDSAPI_SPORTS')

        df = self.extract_participants()
        self.upload(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{self.sport}')

        df = self.generate_participants_lookup()
        self.upload(df, f'SILVER_TEAM_LOOKUPS_{self.sport}')

        df = self.extract_scores()
        self.upload(df, f'BRONZE_SCORES_{self.scores_data_source}_{self.sport}')

        event_starts = self.download_event_starts()
        df = self.extract_events(event_starts)
        self.upload(df, f'BRONZE_ODDSAPI_EVENTS_{self.sport}')

        df_events = self.download(f'BRONZE_ODDSAPI_EVENTS_{self.sport}')
        df = self.extract_odds(df_events)
        self.upload(df, f'BRONZE_ODDSAPI_HIST_ODDS_{self.sport}')

        self.transform_events()
        self.transform_scores()
        self.transform_odds()

    def _run_update(self):
        print('here')
        df = self.extract_scores()
        self.upload(df, f'BRONZE_SCORES_{self.scores_data_source}_{self.sport}_UPDATE')


    def run(self):
        if self.pull_type == 'initial':
            if self.download_data_from_s3():
                return
            self._run_initial()
        elif self.pull_type == 'update':
            self._run_update()
        else:
            raise NotImplementedError()


def run(sport: str, pull_type: str = 'initial'):
    api = Run(sport=sport, pull_type=pull_type)
    api.run()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    parser.add_argument('--pull_type', type=str)
    args = parser.parse_args()
    run(sport=args.sport, pull_type=args.pull_type)

