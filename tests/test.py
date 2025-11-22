import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        import boto3
        client = boto3.client('s3')

    def test_model(self):
        from process_gambling.model.etl import Etl
        mdl = Etl(sport='americanfootball_nfl', version='v0')
        df = mdl.download()
        print(df.shape)

