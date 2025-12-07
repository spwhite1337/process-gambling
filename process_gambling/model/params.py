from process_gambling._etl.load import Load




class Params(Load):
    MODEL_PARAMS = {
        'americanfootball_nfl': {
            'v0': {
                'feature_set': 'base',
                'model_type': 'linear_svc',
                'hyper_params': {
                    'param_grid': {
                        'mdl__C': [0.001, 0.003, 0.01, 0.03, 0.1, 0.3]
                    }
                },
                'response_col': 'team_margin_ats_3',
                'val_year': 2024
            }
        }
    }
    
    FEATURE_SETS = {
        'americanfootball_nfl': {
            'base': [
                'team_margin_ats_abs_window_7',
                'opponent_team_margin_ats_abs_window_7',
                'team_margin_ats_abs_window_7_x_opponent_team_margin_ats_abs_window_7',
                'over_margin_abs_window_7',
                'opponent_over_margin_abs_window_7',
                'over_margin_abs_window_7_x_opponent_over_margin_abs_window_7'
            ]
        }
    }

    GROUP_COLS = ['event_id', 'season']

    interactions = [
        ('team_win_window_3', 'opponent_team_win_window_3'),
        ('team_win_window_5', 'opponent_team_win_window_5'),
        ('team_win_window_7', 'opponent_team_win_window_7'),
        ('team_win_ats_window_3', 'opponent_team_win_ats_window_3'),
        ('team_win_ats_window_5', 'opponent_team_win_ats_window_5'),
        ('team_win_ats_window_7', 'opponent_team_win_ats_window_7'),
        ('team_margin_ats_abs_window_3', 'opponent_team_margin_ats_abs_window_3'),
        ('team_margin_ats_abs_window_5', 'opponent_team_margin_ats_abs_window_5'),
        ('team_margin_ats_abs_window_7', 'opponent_team_margin_ats_abs_window_7'),
        ('team_margin_ats_3_window_3', 'opponent_team_margin_ats_3_window_3'),
        ('team_margin_ats_3_window_5', 'opponent_team_margin_ats_3_window_5'),
        ('team_margin_ats_3_window_7', 'opponent_team_margin_ats_3_window_7'),
        ('over_margin_abs_window_3', 'opponent_over_margin_abs_window_3'),
        ('over_margin_abs_window_5', 'opponent_over_margin_abs_window_5'),
        ('over_margin_abs_window_7', 'opponent_over_margin_abs_window_7'),
    ]

    def __init__(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        self.sport = sport
        self.model_version = version
        self.model_params = self.MODEL_PARAMS[sport][version]
        self.features = self.FEATURE_SETS[sport][self.model_params['feature_set']]
        self.response_col = self.model_params['response_col']

