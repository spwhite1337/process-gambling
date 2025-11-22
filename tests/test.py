import unittest

class Tests(unittest.TestCase):

    def test_s3(self):
        import boto3
        client = boto3.client('s3')

