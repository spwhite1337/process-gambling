from process_gambling._etl import Etl


def run(sport: str):
    api = Etl(sport=sport)
    #df = api.extract_sports()
    #api.upload(df, f'BRONZE_ODDSAPI_SPORTS')

    #df = api.extract_participants()
    #api.upload(df, f'BRONZE_ODDSAPI_PARTICIPANTS_{api.sport}')

    #df = api.generate_participants_lookup()
    #api.upload(df, f'SILVER_TEAM_LOOKUPS_{api.sport}')

    #df = api.extract_scores()
    #api.upload(df, f'BRONZE_SCORES_{api.scores_data_source}_{api.sport}')

    #event_starts = api.download_event_starts()
    #df = api.extract_events(event_starts)
    #api.upload(df, f'BRONZE_ODDSAPI_EVENTS_{api.sport}')

    df_events = api.download(f'BRONZE_ODDSAPI_EVENTS_{api.sport}')
    df = api.extract_odds(df_events)
    api.upload(df, f'BRONZE_ODDSAPI_HIST_ODDS_{api.sport}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--sport', type=str)
    args = parser.parse_args()
    run(sport=args.sport)

