import pytz
import datetime
import pandas as pd

from process_gambling._etl.params import Params


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
                for year in range(self.START_YEAR[self.sport], datetime.datetime.now().year):
                    url = self.SPORTS_REF_API[self.sport]
                    df_ = pd.read_html(f'{url}/teams/{team}/{year}.htm')
                    df.append(df_[1].assign(year=year, team=team))
                    # To avoid 429 errors, wait between pulls
                    # https://www.sports-reference.com/bot-traffic.html
                    time.sleep(30)
            return pd.concat(df)

