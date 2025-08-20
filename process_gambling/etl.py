from process_gambling._etl import Etl


def run(sport: str):
    api = Etl(sport=sport)
    df = api.download_sports()
    api.load(df, 'BRONZE_ODDSAPI_SPORTS')
    api.save_to_s3(f'data/{api.DB_NAME}_{api.DB_VERSION}.db', f'code/process_gambling/data/{api.DB_NAME}_{api.DB_VERSION}.db')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    args = parser.parse_args()
    run(sport=args.sport)

