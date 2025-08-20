import os
from typing import Optional, List

import pandas as pd
import sqlite3
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


    def download_events(self) -> List[str]:
        conn = 
        df_dates = pd.read_sql(f"""
            SELECT DISTINCT kickoff_datetime 
            FROM BRONZE_SPORTSREF_BOXSCORES_{SPORT}
            -- Historical ODDS_API data starts at June 6, 2020
            WHERE kickoff_datetime > DATE('2020-06-06')
            ORDER BY kickoff_datetime
            """, conn)
        conn.close()
        kickoff_dates = df_dates['kickoff_datetime'].to_list()

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

