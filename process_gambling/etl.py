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

    def transform(self):
        self.transform_events()
        self.transform_scores()
        self.transform_odds()
    
    def _run(self):
        if self.pull_type == 'initial':
            df = self.extract_sports()
            self.upload(df, f'BRONZE_ODDSAPI_SPORTS')

            df = self.extract_participants()
            self.upload(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{self.sport}')

            df = self.generate_participants_lookup()
            self.upload(df, f'SILVER_TEAM_LOOKUPS_{self.sport}')

        # df = self.extract_scores()
        # self.upload(df, f'BRONZE_SCORES_{self.scores_data_source}_{self.sport}{self.table_appendix}')

        # event_starts = self.download_event_starts()
        # df = self.extract_events(event_starts)
        # self.upload(df, f'BRONZE_ODDSAPI_EVENTS_{self.sport}{self.table_appendix}')

        # df_events = self.download(f'BRONZE_ODDSAPI_EVENTS_{self.sport}{self.table_appendix}')
        # df = self.extract_odds(df_events)
        # self.upload(df, f'BRONZE_ODDSAPI_HIST_ODDS_{self.sport}{self.table_appendix}')

        self.transform()


    def run(self):
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

