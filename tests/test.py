import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        import boto3
        client = boto3.client('s3')

    def test_model(self):
        from process_gambling.model import Model
        mdl = Model(sport='americanfootball_nfl', version='v0')
        df = mdl.download()
        df_train, df_test = mdl.fit_transform(df)
        mdl.train(df_train)
        mdl.validate(df_test)
        mdl.save_model()
        mdl.upload_model()

