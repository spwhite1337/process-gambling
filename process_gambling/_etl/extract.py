import os
from typing import Optional, List

import pandas as pd
import sqlite3
from tqdm import tqdm

from process_gambling._etl.params import Params


class Extract(Params):
    ODDS_API = 'https://api.the-odds-api.com/v4
    SPORTS_REF_API = 'https://www.pro-football-reference.com/'

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
            endpoint = f'/historical/sports/{self.sport}/events
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

    def _parse_sports_ref(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.sport == 'americanfootball_nfl':
            # Drop non-box score rows
            df = df[
                (df['Unnamed: 93_level_0'] != 'Team Totals')
                &
                (df['Defense'] != 'Unnamed: 3_level_1')
                &
                (df['Unnamed: 0_level_0'] != 'Team Totals')
                &
                (df['Unnamed: 9_level_0'] != 'Bye Week')
                &
                (df['Offense'] != 'Playoffs')
            ]
            # Drop columns that are all nan
            df = df.dropna(axis=1, how='all')
            # Rename columns
            df.columns = [
                'kickoff_time',
                'data_set',
                'kickoff_date',
                'kickoff_day_of_week',
                'week_no',
                'team_score',
                'opponent_score',
                'team_first_downs',
                'team_total_yards',
                'team_pass_yards',
                'team_rush_yards',
                'team_turnovers',
                'team_first_downs_d',
                'team_total_yards_d',
                'team_pass_yards_d',
                'team_rush_yards_d',
                'team_turnovers_d',
                'expected_points_offense',
                'expected_points_defense',
                'expected_points_special_teams',
                'w_or_l',
                'overtime',
                'win_loss_record',
                'is_home_team',
                'opponent',
                'team',
                'season',
            ]
            # Convert to boolean
            df['is_home_team'] = df['is_home_team'].isna()
            df['overtime'] = df['overtime'].isna()
            def gen_kickoff_datetime(r):
                if 'January' in r['kickoff_date'] or 'February' in r['kickoff_date']:
                    season = int(r['season']) + 1
                else:
                    season = int(r['season'])
                return r['kickoff_date'] + ', ' + str(season) + ' ' + r['kickoff_time']
            df['kickoff_datetime'] = df.apply(gen_kickoff_datetime, axis=1)

            def parse_datetime(date_str: str) -> datetime:
                # Remove the timezone abbreviation
                cleaned = date_str.replace("ET", "").strip()
                # Parse the string without timezone
                dt = datetime.datetime.strptime(cleaned, "%B %d, %Y %I:%M%p")
                # add tz-info, then convert and remove tz-info
                return pytz.timezone("US/Eastern").\
                    localize(dt).\
                    astimezone(pytz.timezone("UTC")).\
                    replace(tzinfo=None)
            df['kickoff_datetime'] = df['kickoff_datetime'].apply(parse_datetime)
            df['kickoff_timezone'] = 'UTC'

        return df

    def _download_historical_sports_ref(self) -> pd.DataFrame:
        teams = [t['sports_ref_name'] for t in self.PARTICIPANTS_LOOKUP[self.sport]]
        df = []
        print(f'Downloading Historical Box Scores for {self.sport}')
        for team in tqdm(teams):
            for year in range(2015, datetime.datetime.now().year):
                df_ = pd.read_html(f'https://www.pro-football-reference.com/teams/{team}/{year}.htm')
                df.append(df_[1].assign(year=year, team=team))
                # To avoid 429 errors, wait between pulls
                # https://www.sports-reference.com/bot-traffic.html
                time.sleep(30)
        return pd.concat(df)

