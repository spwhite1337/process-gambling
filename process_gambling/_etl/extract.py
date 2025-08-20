import os
import datetime
from typing import Optional, List, Dict, Union

import requests
import pandas as pd
import sqlite3
from tqdm import tqdm

from process_gambling._etl.helpers import ExtractionHelpersSportsRef


class Extract(ExtractionHelpersSportsRef):
    ODDS_API = 'https://api.the-odds-api.com/v4'

    def download_sports(self) -> pd.DataFrame:
        endpoint = '/sports'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json())

    def download_participants(self) -> pd.DataFrame:
        endpoint = f'/sports/{self.sport}/participants'
        r = requests.get(self.ODDS_API + endpoint, params={'apiKey': self.ODDS_API_KEY})
        return pd.DataFrame.from_records(r.json()).assign(sport=self.sport)

    def generate_participant_lookup(self) -> pd.DataFrame:
        if self.sport in self.PARTICIPANTS_LOOKUP.keys():
            return pd.DataFrame.from_records(self.PARTICIPANTS_LOOKUP[self.sport])
        else:
            print(f'No participants look-up for {self.sport}')
            return pd.DataFrame()

    def download_scores(self) -> pd.DataFrame:
        if self.sport in ['americanfootball_nfl']:
            df = self._download_historical_sports_ref()
            df = self._parse_sports_ref(df)
        else:
            df = pd.DataFrame()
        return df

    def _get_event_starts(self) -> List[str]:
        # Get event-starts for parameters in the ODDS_API
        if self.sport == 'americanfootball_nfl':
            conn = self.connect_to_db()
            df = pd.read_sql(f"""
                SELECT DISTINCT kickoff_datetime
                FROM BRONZE_SPORTSREF_BOXSCORES_{self.sport}
                -- Historical ODDS_API data starts at June 6, 2020
                WHERE kickoff_datetime > DATE('2020-06-06')
                ORDER BY kickoff_datetime
                """, conn)
            self.close_db(conn)
            event_starts = df['kickoff_datetime'].to_list()
        return event_starts

    def download_events(self) -> List[str]:
        # Get date-strings of each event-start-time
        event_starts = self._get_event_starts()
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

    @staticmethod
    def sub_n_days(dt: str, n: int = 3):
        dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(days=n)
        return str('T'.join(str(dt).split(' ')) + 'Z')

    @staticmethod
    def parse_odds_output(r: Dict[str, Union[str, List]) -> List[Dict[str, str]]:
        records = []
        # Parse top-level attributes
        timestamp = r.json()['timestamp']
        previous_timestamp = r.json()['previous_timestamp']
        next_timestamp = r.json()['next_timestamp']
        # Dive into data
        data = r.json()['data']
        ## Parse 2nd-level attributes
        event_id = data['id']
        sport_key = data['sport_key']
        sport_title = data['sport_title']
        commence_time = data['commence_time']
        home_team = data['home_team']
        away_team = data['away_team']
        ## Dive into 2nd-level
        bookmakers = data['bookmakers']
        for bookmaker in bookmakers:
            # Parse attributes
            bookmaker_key = bookmaker['key']
            bookmaker_title = bookmaker['title']
            bookmaker_last_update = bookmaker['last_update']
            # Dive into 3rd-level
            markets = bookmaker['markets']
            for market in markets:
                market_key = market['key']
                market_last_update = market['last_update']
                # Dive into 4th-level
                outcomes = market['outcomes']
                for outcome in outcomes:
                    outcome_name = outcome['name']
                    outcome_price = outcome['price']
                    outcome_point = outcome.get('point')

                    record = {
                        'timestamp': timestamp,
                        'previous_timestamp': previous_timestamp,
                        'next_timestamp': next_timestamp,
                        'event_id': event_id,
                        'sport_key': sport_key,
                        'sport_title': sport_title,
                        'commenct_time': commence_time,
                        'home_team': home_team,
                        'away_team': away_team,
                        'bookmaker_key': bookmaker_key,
                        'bookmaker_title': bookmaker_title,
                        'bookmaker_last_update': bookmaker_last_update,
                        'market_key': market_key,
                        'market_last_update': market_last_update,
                        'outcome_name': outcome_name,
                        'outcome_price': outcome_price,
                        'outcome_point': outcome_point
                    }
                    records.append(record)
        return records


    def download_odds(self) -> pd.DataFrame:
        df = []
        for _, r in tqdm(df_events.iterrows(), total=df_events.shape[0]):
            event_id = r['id']
            commence_time_0 = r['commence_time']
            days_back = [0, 1, 3, 7]
            for day_back in days_back:
                commence_time = sub_n_days(commence_time_0, day_back)
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
                    output = parse_odds_output(res)
                df_ = pd.DataFrame.from_records(output).\
                    assign(days_back=day_back, event_id=event_id, commence_time_0=commence_time_0)
                time.sleep(0.1)
                df.append(df_)
        df = pd.concat(df)
        return df

