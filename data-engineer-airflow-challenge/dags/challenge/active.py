import os
import io
import sys
import csv
import boto3
import logging
import itertools
from datetime import datetime
from collections import defaultdict
from newsapi.newsapi_client import (NewsApiClient, NewsAPIException)


log_format = '%(asctime)s | %(levelname)s | %(message)s'
log_date = datetime.strftime(datetime.now().date(), '%Y%m%d')
log_name = '{}.{}.log'.format(__file__, log_date)
logging.basicConfig(filename=log_name,
                    level=logging.DEBUG, format=log_format)


class Headlines(object):
    '''Access daily articles via api and create tabular files to store in S3.

       Exceptions: 
       NewsAPIException triggered when subscribed is not a paid customer; this limits number of results
       S3Exception triggered when old bucket deleted but not cleaned up by AWS; needs unique bucket name
    '''

    def __init__(self, api_key):
        super(Headlines, self).__init__()
        self.news_cli = NewsApiClient(api_key=api_key)
        self.region = 'us-west-1'
        self.upload_cli = boto3.resource('s3', self.region)
        self.file_name = '_'.join([log_date, 'top_headlines.csv'])
        self.bucket_dict = defaultdict(list)
        self.en_sources = []
        self.headlines = []
        self.paths = []

    def get_english_sources(self) -> list:
        # this will return a complete dataset of 'en' news sources, not paginated
        self.en_sources = sorted([item['id'] for item in
                                  self.news_cli.get_sources(language='en')['sources']])
        logging.debug('SOURCES ({}): {}'.format(len(self.en_sources), self.en_sources))
        return self.en_sources

    def get_keyword_headlines(self, keywords) -> list:
        # this will also return a complete dataset of news sources by keywords, not paginated
        search = ','.join(keywords)
        logging.debug('KEYWORDS ({}): {}'.format(len(keywords), search))
        return self.get_top_headlines(search)

    def get_top_headlines(self, keywords=None) -> list:
        # for all above sources and/or keywords
        page_num = 1
        sources = ','.join(self.en_sources)
        get_top = self.news_cli.get_top_headlines
        if keywords:
            headlines = get_top(sources=sources, q=keywords)
        else:
            headlines = get_top(sources=sources)
        # we need the total results to help us paginate (NOTE: unless paid subscriber, only the first 100 will be retrievable)
        num_results = headlines['totalResults']
        while len(self.headlines) < num_results:
            try:
                results = get_top(sources=sources, page_size=100, page=page_num)
                self.headlines.extend(results['articles'])
                logging.debug(('TOTAL_ARTICLES: {}, PAGE: {}, NUM_PAGE_RESULTS: {}, '
                               'NUM_AGGD_ARTICLES: {}'.format(num_results, page_num, len(results['articles']),
                                                              len(self.headlines))))
                page_num += 1
            except NewsAPIException as ex:
                # this occurs when we have not paid for subscription and we need to short circuit to complete proc
                logging.warning('Caught NewsAPI Exception: {}'.format(ex))
                break

        return self.headlines

    def create_csvs(self) -> list:
        # dynamically gen fields by flattening nested vars
        fields = list(itertools.chain(*[[k] if not isinstance(v, dict) else
                      map(lambda x: '_'.join([k, x]), v.keys()) for k, v in
                      self.headlines[0].items()]))
        logging.debug('FIELDS ({}): {}'.format(len(fields), fields))
        # munge fields to account for flattened nested vars
        for headline in self.headlines:
            headline['content_id'] = headline['source']['id']
            headline['content_name'] = headline['source']['name']
            del headline['source']
            self.bucket_dict[headline['content_id']].append(headline)
        logging.debug('BUCKETS ({}): {}, {}'.format(len(self.bucket_dict.keys()),
                                                    self.bucket_dict.keys(), self.bucket_dict))
        for bucket, headlines in self.bucket_dict.items():
            # tmp file store for ease of bucket naming
            path = '@'.join([self.file_name, bucket])
            with open(path, 'w') as out:
                writer = csv.DictWriter(out, fieldnames=fields)
                writer.writeheader()
                for row in headlines:
                    writer.writerow(row)
            self.paths.append(path)
            logging.debug('PATHS ({}): {}'.format(len(self.paths), self.paths))
        return self.paths

    def upload_s3(self):
        for path in self.paths:
            out_file, bucket = path.split('@')
            logging.debug('UPLOADING: {}, TO: {}/{}'.format(path, bucket, self.file_name))
            # first check if bucket already exists
            if not self.upload_cli.Bucket(bucket) in self.upload_cli.buckets.all():
                try:
                    self.upload_cli.create_bucket(Bucket=bucket, ACL='public-read-write',
                                                  CreateBucketConfiguration={'LocationConstraint': self.region})
                except Exception as ex:
                    # S3 needs time to delete old buckets so sometimes we have to force this to be a unique name (at least for testing)
                    self.upload_cli.create_bucket(Bucket='.'.join([bucket, log_date]), ACL='public-read-write',
                                                  CreateBucketConfiguration={'LocationConstraint': self.region})
            try:
                self.upload_cli.meta.client.upload_file(path, bucket, self.file_name)
            except Exception as ex:
                logging.error('Caught upload error: {}'.format(ex))
