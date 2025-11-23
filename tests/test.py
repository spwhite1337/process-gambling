import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        import boto3
        client = boto3.client('s3')

    def test_model(self):
        from process_gambling.model.wrangle import Wrangle
        mdl = Wrangle(sport='americanfootball_nfl', version='v0')
        df = mdl.download()
        df_train, df_test = mdl.fit_transform(df)
        print(df_train.shape)
        print(df_test.shape)

