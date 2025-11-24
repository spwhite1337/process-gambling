



class Params(object):
    MODEL_PARAMS = {
        'americanfootball_nfl': {
            'v0': {
                'feature_set': 'base',
                'model_type': 'linear_svc',
                'hyper_params': {
                    'param_grid': {
                        'mdl__C': [0.003, 0.01, 0.03, 0.1, 0.3]
                    }
                },
                'response_col': 'team_win_ats',
                'val_year': 2024
            }
        }
    }
    
    FEATURE_SETS = {
        'americanfootball_nfl': {
            'base': [
                # 'team_spread_abs',
                # 'over_win_window_3',
                # 'over_win_window_7',
                'team_win_ats_window_3', 
                'team_win_ats_window_7', 
                'team_margin_ats_abs_window_3',
                'team_margin_ats_abs_window_7', 
                'opponent_team_win_ats_window_3', 
                'opponent_team_win_ats_window_7', 
                'opponent_team_margin_ats_abs_window_3', 
                'opponent_team_margin_ats_abs_window_7',
            ]
        }
    }

    GROUP_COLS = ['event_id', 'season']

    def __init__(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        self.sport = sport
        self.model_version = version
        self.model_params = self.MODEL_PARAMS[sport][version]
        self.features = self.FEATURE_SETS[sport][self.model_params['feature_set']]
        self.response_col = self.model_params['response_col']

