import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        import boto3
        client = boto3.client('s3')

    def test_train_model(self):
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0')
        df = mdl.download_train()
        df_train, df_test = mdl.fit_transform(df)
        mdl.train(df_train)
        mdl.validate(df_test)

    def test_load_model(self):
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0')
        df = mdl.download_train()
        _, df_test = mdl.fit_transform(df)
        mdl = mdl.load_model()
        mdl.validate(df_test)

    def test_update_preds(self):
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0').load_model()
        df = mdl.download_update()
        mdl.validate(df)

