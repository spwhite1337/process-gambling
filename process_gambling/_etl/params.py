import os
import sqlite3


class Params(object):
    ODDS_API_KEY = os.environ.get('ODDS_API_KEY')
    DB_NAME = 'process_gambling'
    DB_VERSION = 'v0'
    VALID_SPORTS = [
        'americanfootball_nfl'
    ]

    PARTICIPANTS_LOOKUP = {
        'americanfootball_nfl': [
            {'sports_odds_name': 'Arizona Cardinals', 'sports_ref_name': 'crd'},
            {'sports_odds_name': 'Atlanta Falcons', 'sports_ref_name': 'atl'},
            {'sports_odds_name': 'Baltimore Ravens', 'sports_ref_name': 'rav'},
            {'sports_odds_name': 'Buffalo Bills', 'sports_ref_name': 'buf'},
            {'sports_odds_name': 'Carolina Panthers', 'sports_ref_name': 'car'},
            {'sports_odds_name': 'Chicago Bears', 'sports_ref_name': 'chi'},
            {'sports_odds_name': 'Cincinnati Bengals', 'sports_ref_name': 'cin'},
            {'sports_odds_name': 'Cleveland Browns', 'sports_ref_name': 'cle'},
            {'sports_odds_name': 'Dallas Cowboys', 'sports_ref_name': 'dal'},
            {'sports_odds_name': 'Denver Broncos', 'sports_ref_name': 'den'},
            {'sports_odds_name': 'Detroit Lions', 'sports_ref_name': 'det'},
            {'sports_odds_name': 'Green Bay Packers', 'sports_ref_name': 'gnb'},
            {'sports_odds_name': 'Houston Texans', 'sports_ref_name': 'htx'},
            {'sports_odds_name': 'Indianapolis Colts', 'sports_ref_name': 'clt'},
            {'sports_odds_name': 'Jacksonville Jaguars', 'sports_ref_name': 'jax'},
            {'sports_odds_name': 'Kansas City Chiefs', 'sports_ref_name': 'kan'},
            {'sports_odds_name': 'Las Vegas Raiders', 'sports_ref_name': 'rai', 'alt_name_1': 'Oakland Raiders'},
            {'sports_odds_name': 'Los Angeles Chargers', 'sports_ref_name': 'sdg', 'alt_name_1': 'San Diego Chargers'},
            {'sports_odds_name': 'Los Angeles Rams', 'sports_ref_name': 'ram', 'alt_name_1': 'St. Louis Rams'},
            {'sports_odds_name': 'Miami Dolphins', 'sports_ref_name': 'mia'},
            {'sports_odds_name': 'Minnesota Vikings', 'sports_ref_name': 'min'},
            {'sports_odds_name': 'New England Patriots', 'sports_ref_name': 'nwe'},
            {'sports_odds_name': 'New Orleans Saints', 'sports_ref_name': 'nor'},
            {'sports_odds_name': 'New York Giants', 'sports_ref_name': 'nyg'},
            {'sports_odds_name': 'New York Jets', 'sports_ref_name': 'nyj'},
            {'sports_odds_name': 'Philadelphia Eagles', 'sports_ref_name': 'phi'},
            {'sports_odds_name': 'Pittsburgh Steelers', 'sports_ref_name': 'pit'},
            {'sports_odds_name': 'Seattle Seahawks', 'sports_ref_name': 'sea'},
            {'sports_odds_name': 'San Francisco 49ers', 'sports_ref_name': 'sfo'},
            {'sports_odds_name': 'Tampa Bay Buccaneers', 'sports_ref_name': 'tam'},
            {'sports_odds_name': 'Tennessee Titans', 'sports_ref_name': 'oti'},
            {'sports_odds_name': 'Washington Commanders', 'sports_ref_name': 'was', 'alt_name_1': 'Washington Redskins', 'alt_name_2': 'Washington Football Team'},
        ]
    }

    def __init__(self, sport: Optional[str] = None):
        if sport is not None:
            assert sport in self.VALID_SPORTS
        self.sport = sport
        if not self.ODDS_API_KEY:
            print('WARNING: No ODDS-API Key')

    def connect_to_db(self):
        if not os.path.exists('data'):
            os.makedirs('data')
        return sqlite3.connect(f'data/{self.DB_NAME}_{self.DB_VERSION}.db')

    @staticmethod
    def close_db(conn):
        conn.close()

