import pandas as pd
from tqdm import tqdm
from process_gambling.etl.params import Params


class Extract(Params):

    def _download_historical_nfl(self) -> pd.DataFrame:
        df = []
        for team in tqdm(teams):
            for year in range(2015, 2025):
        df_ = pd.read_html(f'https://www.pro-football-reference.com/teams/{team}/{year}.htm')
        df.append(df_[1].assign(year=year, team=team))
        # To avoid 429 errors, wait between pulls
        # https://www.sports-reference.com/bot-traffic.html
        time.sleep(30)
        df = pd.concat(df)
        return df

