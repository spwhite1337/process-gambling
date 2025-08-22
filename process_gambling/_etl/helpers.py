from typing import Dict, List, Union
import time
import pytz
import datetime
import pandas as pd
from tqdm import tqdm

from process_gambling._etl.params import Params
from process_gambling.config import logger


class ExtractionHelpersSportsRef(Params):
    START_YEAR = {
        'americanfootball_nfl': 2015
    }
    SPORTS_REF_API = {
        'americanfootball_nfl': 'https://www.pro-football-reference.com/'
    }


    # Sports-Reference
    def _parse_sports_ref(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.sport == 'americanfootball_nfl':
            df.columns = [
                'week_no',
                'kickoff_day_of_week',
                'kickoff_date',
                'kickoff_time',
                'data_set',
                'w_or_l',
                'overtime',
                'win_loss_record',
                'is_home_team',
                'opponent',
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
                'season',
                'team',
            ]
            # Convert to boolean
            df['is_home_team'] = df['is_home_team'].isna()
            df['overtime'] = ~df['overtime'].isna()
            # Drop non-game rows
            df = df[(df['opponent'] != 'Bye Week') & (df['kickoff_date'] != 'Playoffs')].copy()

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
        logger.info(f'Downloading Historical Box Scores for {self.sport}')
        for team in tqdm(teams):
            for year in range(self.START_YEAR[self.sport], datetime.datetime.now().year):
                url = self.SPORTS_REF_API[self.sport]
                df_ = pd.read_html(f'{url}/teams/{team}/{year}.htm')
                df.append(df_[1].assign(year=year, team=team))
                # To avoid 429 errors, wait between pulls
                # https://www.sports-reference.com/bot-traffic.html
                time.sleep(30)
        return pd.concat(df)


class ExtractionHelpersOddsApi(object):
    ODDS_API = 'https://api.the-odds-api.com/v4'

    @staticmethod
    def _sub_n_days(dt: str, n: int = 3):
        dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(days=n)
        return str('T'.join(str(dt).split(' ')) + 'Z')

    @staticmethod
    def _parse_odds_output(r: Dict[str, Union[str, List]]) -> List[Dict[str, str]]:
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
                    # Gather for record
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


class TransformHelpers(object):

    manual_imputes_events = """
        UNION
        SELECT
            '81c8578ea74ac6db74ba12d9f694e9e2' event_id,
            '2020-10-04 17:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-04 17:00:00') query_date,
            'chi' team,
            'Chicago Bears' team_name,
            2020 season
        UNION
        SELECT
            '81c8578ea74ac6db74ba12d9f694e9e2' event_id,
            '2020-10-04 17:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-04 17:00:00') query_date,
            'clt' team,
            'Indianapolis Colts' team_name,
            2020 season
        UNION
        SELECT
            '4f9e131b7dca1395f726a9d8c14bed7a' event_id,
            '2020-10-05 23:05:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-05 23:05:00') query_date,
            'kan' team,
            'Kansas City Chiefs' team_name,
            2020 season
        UNION
        SELECT
            '4f9e131b7dca1395f726a9d8c14bed7a' event_id,
            '2020-10-05 23:05:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2020-10-05 23:05:00') query_date,
            'nwe' team,
            'New England Patriots' team_name,
            2020 season
        UNION
        SELECT
            '1c49d40e4090e0eb9c4d0cab2c6fc272' event_id,
            '2021-12-05 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:25:00') query_date,
            'ram' team,
            'Los Angeles Rams',
            2021 season
        UNION
        SELECT
            '1c49d40e4090e0eb9c4d0cab2c6fc272' event_id,
            '2021-12-05 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:25:00') query_date,
            'jax' team,
            'Jacksonville Jaguars',
            2021 season
        UNION
        SELECT
            'd1083bd9225f59b8444e7c00426eeed4' event_id,
            '2021-12-05 21:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:00:00') query_date,
            'sea' team,
            'Seattle Seahawks' team_name,
            2021 season
        UNION
        SELECT
            'd1083bd9225f59b8444e7c00426eeed4' event_id,
            '2021-12-05 21:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-05 21:00:00') query_date,
            'sfo' team,
            'San Francisco 49ers' team_name,
            2021 season
        UNION
        SELECT
            '9766c28eb9d0ba3b1afe1d9ec2cb00ae' event_id,
            '2021-12-12 18:00:00' kickoff_datetime,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'cin' team,
            'Cincinnati Bengals' team_name,
            2021 season
        UNION
        SELECT
            '9766c28eb9d0ba3b1afe1d9ec2cb00ae' event_id,
            '2021-12-12 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'sfo' team,
            'San Francisco 49ers' team_name,
            2021 season
        UNION
        SELECT
            '45a9db10794fbfb4a4300351a1f2a9d7' event_id,
            '2022-01-02 21:25:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 21:25:00') query_date,
            'rav' team,
            'Baltimore Ravens' team_name,
            2021 season
        UNION
        SELECT
            '45a9db10794fbfb4a4300351a1f2a9d7' event_id,
            '2021-12-12 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2021-12-12 18:00:00') query_date,
            'ram' team,
            'Los Angeles Rams' team_name,
            2021 season
        UNION
        SELECT
            'ec40d8338952a9798cd12b44c90d1db4' event_id,
            '2022-01-02 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 18:00:00') query_date,
            'nor' team,
            'New Orleans Saints' team_name,
            2021 season
        UNION
        SELECT
            'ec40d8338952a9798cd12b44c90d1db4' event_id,
            '2022-01-02 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-02 18:00:00') query_date,
            'car' team,
            'Carolina Panthers' team_name,
            2021 season
        UNION
        SELECT
            '14a8edc98fc58e8f6ae30aaed7b5c1fc' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'mia' team,
            'Miami Dolphins' team_name,
            2021 season
        UNION
        SELECT
            '14a8edc98fc58e8f6ae30aaed7b5c1fc' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'nwe' team,
            'New England Patriots' team_name,
            2021 season
        UNION
        SELECT
            'db65d83c1b4b4d7eed4a953ee05ff059' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'atl' team,
            'Atlanta Falcons' team_name,
            2021 season
        UNION
        SELECT
            'db65d83c1b4b4d7eed4a953ee05ff059' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'nor' team,
            'New Orleans Saints' team_name,
            2021 season
        UNION
        SELECT
            '672e109ed4d235a5502ce414277db27e' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'tam' team,
            'Tampa Bay Buccaneers' team_name,
            2021 season
        UNION
        SELECT
            '672e109ed4d235a5502ce414277db27e' event_id,
            '2022-01-09 18:00:00' event_start,
            STRFTIME('%Y-%m-%dT%H:%M:%SZ', '2022-01-09 18:00:00') query_date,
            'car' team,
            'Carolina Panthers' team_name,
            2021 season
    """
