import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline, Pipeline

from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC

from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import GroupKFold

from process_gambling.model.wrangle import Wrangle


class Train(Wrangle):

    def __init(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        super().__init__(sport=sport, version=version)
        self.mdl = None

    def train(self, df: pd.DataFrame):
        self.mdl = GridSearchCV(
            Pipeline([
                ('standardscaler', StandardScaler()),
                ('mdl', SVC(max_iter=-1, probability=True, kernel='linear'))
            ]),
            verbose=1,
            scoring='roc_auc',
            param_grid={
                'mdl__C': [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1]
            },
            cv=GroupKFold(n_splits=df['season'].nunique()),
            return_train_score=True,
            refit=True
        )
        self.mdl.fit(df[self.features], df[self.response_col], groups=df['season'])
        df_cv = pd.DataFrame(self.mdl.cv_results_)

