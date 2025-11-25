import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline, Pipeline

from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC

from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import GroupKFold

from process_gambling.model.wrangle import Wrangle
from process_gambling import MODEL_VERSION


class Train(Wrangle):

    def __init(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        super().__init__(sport=sport, version=version)
        self.mdl = None

    def train(self, df: pd.DataFrame):
        if self.model_params['model_type'] == 'linear_svc':
            ppl = Pipeline([
                ('standardscaler', StandardScaler()),
                ('mdl', SVC(max_iter=-1, probability=True, kernel='linear', random_state=187)
                )
            ])
        self.mdl = GridSearchCV(
            ppl,
            verbose=1,
            scoring='roc_auc',
            param_grid=self.model_params['hyper_params']['param_grid'],
            cv=GroupKFold(n_splits=df['season'].nunique()),
            return_train_score=True,
            refit=True,
        )
        self.mdl.fit(df[self.features], df[self.response_col], groups=df['season'])
        df_cv = pd.DataFrame(self.mdl.cv_results_)
        df_cv['params'] = df_cv['params'].astype(str)
        self.upload(df_cv, f'GOLD_CV_RESULTS_MODEL_{MODEL_VERSION}')
        
