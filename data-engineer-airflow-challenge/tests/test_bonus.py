#!/usr/bin/env python

from dags.challenge.active import Headlines
import os
import sys
import unittest
sys.path.append('../')


HEAD = Headlines(os.environ['API_KEY'])


class BonusTests(unittest.TestCase):
    def test_keyword_articles(self):
        keywords = ['Tempus Labs', 'Eric Lefkofsky', 'Cancer', 'Immunotherapy']
        HEAD.get_english_sources()
        HEAD.up_sources = set(HEAD.bucket_dict.keys())
        HEAD.get_keyword_headlines(keywords)
        up_articles = set()
        for bucket in HEAD.up_sources:
            file_obj = HEAD.upload_cli.Bucket(bucket).objects.filter(Prefix=HEAD.file_name)
            for obj in file_obj:
                print('BODY1: {}'.format(obj.get('Body')))
                print('BODY2: {}'.format(obj.get()['Body']))
                reader = csv.DictReader(io.BytesIO(obj.get('Body')))
                print('READER: {}'.format(reader))
                up_articles.update([row['url'] for row in reader])
        expect_articles = set([item for sub in [article.url for article in
                               HEAD.bucket_dict.values()] for item in sub])
        missing_articles = expect_articles.difference(up_articles)
        self.assertTrue(not missing_articles)

    def test_buckets(self):
        HEAD.create_csvs()
        HEAD.upload_s3()
        up_buckets = set([bucket.name for bucket in HEAD.upload_cli.buckets.all()])
        expect_buckets = set(HEAD.bucket_dict.keys())
        missing_buckets = expect_buckets.difference(up_buckets)
        self.assertTrue(not missing_buckets)


if __name__ == '__main__':
    unittest.main()
