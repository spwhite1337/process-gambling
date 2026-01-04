from typing import Optional
import pandas as pd
import numpy as np
from scipy.stats import binomtest

from sklearn.metrics import roc_auc_score, roc_curve

from process_gambling.model.train import Train


class Validate(Train):

    def show_cv(self):
        conn = self.connect_to_db()
        df = pd.read_sql(f'SELECT * FROM GOLD_CV_RESULTS_MODEL_{self.model_version}', conn)
        df = df[df['rank_test_score'] == 1]
        print(df.transpose())

    def validate(self, df: pd.DataFrame, table_name: Optional[str] = None):

        if self.mdl is None:
            raise FileNotFoundError('Model not trained yet')

        # CV results
        self.show_cv()

        # Get preds
        df = self._transform(df)
        val_preds = self.mdl.predict_proba(df[self.features])[:, 1]
        val_trues = df[self.response_col]
        df_val = pd.DataFrame({
            'trues': val_trues,
            'preds': val_preds
        })

        auc = roc_auc_score(val_trues, val_preds)
        print(f'AUC: {round(auc, 3)}')

        # Define perecentiles based on val-set and later prediction categories
        if self.ptiles is None:
            self.ptiles = np.percentile(df_val['preds'].values, np.arange(0, 100, 11))

        null_hyp = df_val['trues'].mean()
        print(f'null_hyp: {round(null_hyp, 3)}')
        for th in self.ptiles:
            wins = (df_val[df_val['preds'] >= th]['trues']).sum()
            losses = (1-df_val[df_val['preds'] >= th]['trues']).sum()
            if wins+losses > 0:
                pvalue = binomtest(wins, wins+losses, null_hyp, 'greater').pvalue
                print(f'U: th: {round(th, 3)}, {wins}, {losses}, {round(wins/(wins+losses), 3)}, pvalue: {round(pvalue, 3)}')

            wins = (1-df_val[df_val['preds'] <= th]['trues']).sum()
            losses = (df_val[df_val['preds'] <= th]['trues']).sum()
            if wins+losses > 0:
                pvalue = binomtest(wins, wins+losses, 1-null_hyp, 'less').pvalue
                print(f'L: th: {round(th, 3)}, {wins}, {losses}, {round(wins/(wins+losses), 3)}, pvalue: {round(pvalue, 3)}')
            print('')
                

        if table_name is not None:
            self.upload(df_val, table_name)

