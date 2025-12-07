from typing import Optional
import pandas as pd
import numpy as np
from scipy.stats import binomtest

from process_gambling.model.train import Train


class Validate(Train):

    def validate(self, df: pd.DataFrame, table_name: Optional[str] = None):
        if self.mdl is None:
            raise FileNotFoundError('Model not trained yet')
        val_preds = self.mdl.predict_proba(df[self.features])[:, 1]
        val_trues = df[self.response_col]
        df_val = pd.DataFrame({
            'trues': val_trues,
            'preds': val_preds
        })

        # Define perecentiles based on val-set
        if self.ptiles is None:
            self.ptiles = np.percentile(df_val['preds'].values, np.arange(0, 100, 11))

        null_hyp = df_val['trues'].mean()
        for th in self.ptiles:
            wins = (df_val[df_val['preds'] >= th]['trues']).sum()
            losses = (1-df_val[df_val['preds'] >= th]['trues']).sum()
            if wins+losses > 0:
                pvalue = binomtest(wins, wins+losses, null_hyp).pvalue
                print(f'U: th: {round(th, 3)}, {wins}, {losses}, {round(wins/(wins+losses), 3)}, pvalue: {round(pvalue, 3)}')

            wins = (1-df_val[df_val['preds'] <= th]['trues']).sum()
            losses = (df_val[df_val['preds'] <= th]['trues']).sum()
            if wins+losses > 0:
                pvalue = binomtest(wins, wins+losses, 1-null_hyp).pvalue
                print(f'L: th: {round(th, 3)}, {wins}, {losses}, {round(wins/(wins+losses), 3)}, pvalue: {round(pvalue, 3)}')
            print('')
                

        if table_name is not None:
            self.upload(df_val, table_name)

