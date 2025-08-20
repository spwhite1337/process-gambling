from process_gambling._etl import Etl


def run(sport: str):
    api = Etl(sport=sport)
    df = api.download_sports()
    api.load(df, f'BRONZE_ODDSAPI_SPORTS')

    df = api.download_participants()
    api.load(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{api.sport}'

    df = api.generate_participants_lookup()
    api.load(df, f'SILVER_TEAM_LOOKUPS_{api.sport}')

    df = api.download_scores()
    api.load(df, f'BRONZE_SCORES_{api.SCORES_DATA_SOURCE}_{api.sport}')

    df = api.download_events()
    api.load(df, f'BRONZE_ODDSAPI_EVENTS_{api.sport}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    args = parser.parse_args()
    run(sport=args.sport)

