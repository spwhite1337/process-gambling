import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        print('Testing IAM')
        import boto3
        client = boto3.client('s3')

    def test_train_model(self):
        print('Testing Model Train')
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0')
        df = mdl.download_train()
        df_train, df_test = mdl.fit_transform(df)
        mdl.train(df_train)
        mdl.validate(df_test)
        mdl.save_model()
        # mdl.upload_model()

    def test_update_preds(self):
        print('Testing Preds from Loaded Model and Update Data')
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0')
        # mdl.download_model()
        mdl = mdl.load_model()
        df = mdl.download_update()
        mdl.validate(df, 'GOLD_UPDATE_PREDS_v0')

