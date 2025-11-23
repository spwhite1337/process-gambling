import pandas as pd
import numpy as np
from scipy.stats import binomtest

from process_gambling.model.train import Train


class Validate(Train):

    def validate(self, df: pd.DataFrame):
        if self.mdl is None:
            raise FileNotFoundError('Model not trained yet')
        val_preds = self.mdl.predict_proba(df[self.features])[:, 1]
        val_trues = df[self.response_col]
        df_plot = pd.DataFrame({
            'trues': val_trues,
            'preds': val_preds
        })
        for th in np.linspace(val_preds.min(), val_preds.max(), 10):
            wins = (1-df_plot[df_plot['preds'] <= th]['trues']).sum()
            losses = (df_plot[df_plot['preds'] <= th]['trues']).sum()
            pvalue = binomtest(wins, wins+losses, 0.5).pvalue
            print(f'th: {round(th, 3)}, {wins}, {losses}, {round(wins/(wins+losses), 3)}, pvalue: {round(pvalue, 2)}')

