import os
from typing import Optional
import pandas as pd
from tqdm import tqdm
from process_gambling._etl.params import Params


class Extract(Params):
    ODDS_API = 'https://api.the-odds-api.com/v4
    ODDS_API_KEY = os.environ.get('ODDS_API_KEY')
    SPORTS_REF_API = 'https://www.pro-football-reference.com/'


    def __init__(self, sport: Optional[str]):
        if sport is not None:
            assert sport in self.VALID_SPORTS
        self.sport = sport
        if not self.ODDS_API_KEY:
            print('WARNING: No ODDS-API Key')


    def download_sports(self) -> pd.DataFrame:
        endpoint = '/sports'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json())

    def download_participants(self) -> pd.DataFrame:
        # Get participants
        endpoint = f'/sports/{self.sport}/participants'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json()).assign(sport=self.sport)

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

