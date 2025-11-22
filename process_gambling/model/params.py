



class Params(object):
    MODEL_PARAMS = {
        'americanfootball_nfl': {
            'v0': {
                'model_type': 'linear_svc',
                'hyper_params': {
                    'penalty': 'l2',
                    'max_iter': -1,
                    'probability': True
                }
            }
        }
    }

    def __init__(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        self.sport = sport
        self.model_version = version

