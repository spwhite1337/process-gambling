import os
import datetime
from typing import Optional, List, Dict, Union

import requests
import pandas as pd
import sqlite3
from tqdm import tqdm

from process_gambling._etl.helpers import ExtractionHelpersSportsRef, ExtractionHelpersOddsApi
from process_gambling.config import logger


class Extract(ExtractionHelpersSportsRef, ExtractionHelpersOddsApi):


    def __init__(self, sport: str):
        super().__init__(sport=sport)
        # Validate authentication for endpoints
        ## Odds-API
        r = requests.get(self.ODDS_API + '/sports', params={'apiKey': self.ODDS_API_KEY})
        self.odds_api_auth = r.status_code == 200

    def check_credentials(self, api: Optional[str] = None):
        if api is None:
            return

        if api == 'odds-api':
            if not self.odds_api_auth:
                raise Exception('Not Authenticated for ODDS-API')

    def extract_sports(self) -> pd.DataFrame:
        self.check_credentials(api='odds-api')
        logger.info('Extracting Sports')
        endpoint = '/sports'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json())

    def extract_participants(self) -> pd.DataFrame:
        self.check_credentials(api='odds-api')
        logger.info(f'Extracting Participants in {self.sport}')
        endpoint = f'/sports/{self.sport}/participants'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json()).assign(sport=self.sport)

    def generate_participants_lookup(self) -> pd.DataFrame:
        logger.info(f'Generating Participant Lookup for {self.sport}')
        if self.sport in self.PARTICIPANTS_LOOKUP.keys():
            return pd.DataFrame.from_records(self.PARTICIPANTS_LOOKUP[self.sport])
        else:
            print(f'No participants look-up for {self.sport}')
            return pd.DataFrame()

    def extract_scores(self) -> pd.DataFrame:
        if self.sport in ['americanfootball_nfl']:
            df = self._download_historical_sports_ref()
            df = self._parse_sports_ref(df)
        else:
            df = pd.DataFrame()
        return df

    def extract_events(self, event_starts: List[str]) -> List[str]:
        self.check_credentials(api='odds-api')
        # Pull all events at the listed kickoff-dates
        df = []
        for event_start in event_starts:
            date = 'T'.join(event_start.split(' ')) + 'Z'
            # Start with a 10-minute window around the kickoff time from scores data
            commenceTimeFrom = 'T'.join(str(datetime.datetime.strptime(event_start, '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=10)).split(' ')) + 'Z'
            commenceTimeTo = 'T'.join(str(datetime.datetime.strptime(event_start, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(minutes=10)).split(' ')) + 'Z'
            endpoint = f'/historical/sports/{self.sport}/events'
            r = requests.get(
                self.ODDS_API + endpoint,
                params={
                    'apiKey': self.ODDS_API_KEY,
                    'date': date,
                    'commenceTimeFrom': commenceTimeFrom,
                    'commenceTimeTo': commenceTimeTo
                }
            )
            # If it returns nothing, expand the window to 100 minutes
            if len(r.json()['data']) == 0:
                commenceTimeFrom = 'T'.join(str(datetime.datetime.strptime(event_start, '%Y-%m-%d %H:%M:%S') - datetime.timedelta(minutes=100)).split(' ')) + 'Z'
                commenceTimeTo = 'T'.join(str(datetime.datetime.strptime(event_start, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(minutes=100)).split(' ')) + 'Z'
                r = requests.get(
                    self.ODDS_API + endpoint,
                    params={
                        'apiKey': self.ODDS_API_KEY,
                        'date': date,
                        'commenceTimeFrom': commenceTimeFrom,
                        'commenceTimeTo': commenceTimeTo
                    }
                )
            df_ = pd.DataFrame.from_records(r.json()['data']).assign(
                input_date=r.json()['timestamp'],
                previous_timestamp=r.json()['previous_timestamp'],
                next_timestamp=r.json()['next_timestamp'],
                event_start=event_start,
                query_date=date,
                query_commencetimefrom=commenceTimeFrom,
                query_commencetimeto=commenceTimeTo,
                url=url
            )
            df.append(df_)
            # Sleep to take it easy on the API
            time.sleep(0.1)
        df = pd.concat(df)
        # De-dupe
        df['filter'] = df.groupby('id')['query_date'].transform('max')
        df = df[df['filter'] == df['query_date']].drop('filter', axis=1)
        return df

    def extract_odds(self, df_events: pd.DataFrame) -> pd.DataFrame:
        self.check_credentials(api='odds-api')
        print(f'Extracting Odds for {self.sport}')
        df = []
        for _, r in tqdm(df_events.iterrows(), total=df_events.shape[0]):
            event_id = r['id']
            commence_time_0 = r['commence_time']
            days_back = [0, 1, 3, 7]
            for day_back in days_back:
                commence_time = self._sub_n_days(commence_time_0, day_back)
                endpoint = f'/historical/sports/{self.sport}/events/{event_id}/odds'
                res = requests.get(
                    self.ODDS_API + endpoint,
                    params={
                        'apiKey': self.ODDS_API_KEY,
                        'bookmakers': self.ODDS_API_BOOKMAKERS[self.sport],
                        'markets': self.ODDS_API_MARKETS[self.sport],
                        'date': commence_time
                    }
                )
                if res.json().get('error_code'):
                    output = [{'error': res.json().get('error_code')}]
                else:
                    output = self._parse_odds_output(res)
                df_ = pd.DataFrame.from_records(output).\
                    assign(days_back=day_back, event_id=event_id, commence_time_0=commence_time_0)
                time.sleep(0.1)
                df.append(df_)
        df = pd.concat(df)
        return df

