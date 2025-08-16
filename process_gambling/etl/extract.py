import pandas as pd


class Extract(object):
    PRO_FOOTBALL_REFERENCE_TEAM_CODES = [
        'phi',
        'nyg',
        'was',
        'dal',
        'rai',
        'kan',
        'sdg',
        'den',
        'cle',
        'rav',
        'pit',
        'cin',
        'buf',
        'mia',
        'nwe',
        'nyj',
        'htx',
        'jax',
        'oti',
        'clt',
        'gnb',
        'chi',
        'min',
        'det',
        'nor',
        'tam',
        'car',
        'atl',
        'sea',
        'ram',
        'crd',
        'sfo'
    ]

    def _download_historical_nfl(self) -> pd.DataFrame:
        df = []
        for team in tqdm(teams):
            for year in range(2015, 2025):
        df_ = pd.read_html(f'https://www.pro-football-reference.com/teams/{team}/{year}.htm')
        df.append(df_[1].assign(year=year, team=team))
        # To avoid 429 errors, wait between pulls
        # https://www.sports-reference.com/bot-traffic.html
        time.sleep(30)
        df = pd.concat(df)
        return df

