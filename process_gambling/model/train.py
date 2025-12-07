import pandas as pd

from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline, Pipeline

from sklearn.svm import SVC
from lightgbm import LGBMClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.frozen import FrozenEstimator

from process_gambling.utils.utils import WalkForwardCV
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import GroupKFold
from sklearn.calibration import CalibratedClassifierCV

from process_gambling.model.wrangle import Wrangle
from process_gambling import MODEL_VERSION


class Train(Wrangle):

    def __init(self, sport: str = 'americanfootball_nfl', version: str = 'v0'):
        super().__init__(sport=sport, version=version)
        self.mdl = None
        self.ptiles = None

    def train(self, df: pd.DataFrame):
        if model_type == 'svc_linear':
            mdl_ =  ('mdl', SVC(max_iter=-1, probability=True, kernel='linear', random_state=187))
            hps = {'mdl__C': [0.0001, 0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1]}
        elif model_type == 'logreg':
            mdl_ =  ('mdl', LogisticRegression(
                penalty='l2', 
                solver='liblinear', 
                fit_intercept=True, 
                random_state=187
            ))
            hps = {'mdl__C': [0.01, 0.03, 0.1, 0.3, 1]}
        elif model_type == 'svc_rbf':
            mdl_ =  ('mdl', SVC(
                max_iter=-1, 
                probability=True, 
                kernel='rbf',
                random_state=187
            ))
            hps = {'mdl__C': [0.0001, 0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1]}
        elif model_type == 'lgbm':
            mdl_ = ('mdl', LGBMClassifier(random_state=42))
            hps = {
                'mdl__max_depth': [-1, 3, 4],
                'mdl__n_estimators': [100, 1000],
                'mdl__learning_rate': [0.01, 0.001],
                'mdl__num_leaves': [3, 5, 10],
                'mdl__scale_pos_weight': [1, 10, 100],
            }
        else:
            raise NotImplementedError(model_type)

        # Cv folds
        gkf = GroupKFold(n_splits=df['season'].nunique())
        wfv = WalkForwardCV(n_splits=self.n_splits, n_train=500)

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
        self.ptiles = None
