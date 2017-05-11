"""
Lambda functions for the string_transformer_workflow example workflow.

To deploy:

  rm /tmp/price_data_reader.zip
  zip /tmp/price_data_reader.zip price_data_reader.py

  for name in reader sec_reader thread_reader global_reader; do
    aws lambda delete-function --function-name pdr_price_data_${name}
    aws lambda create-function --function-name pdr_price_data_${name} --runtime python2.7 \
      --role arn:aws:iam::528461152743:role/TradingOpsLambda --handler price_data_reader.${name} \
      --zip-file fileb:///tmp/price_data_reader.zip --publish --timeout 30 --memory-size 1536
  done

To delete the functions:

  for name in reader sec_reader thread_reader global_reader; do
    aws lambda delete-function --function-name pdr_price_data_${name}
  done
"""

import os
import threading
from time import sleep

import boto3

PREFIX = 'pdr/blobz'


def reader(event, context):
    sleep(0.1)
    str = "abcdefghijklmnop"*65536
    return str


def sec_reader(event, context):
    sec_name = event
    category = 'DAILY'
    contract_name = None
    col = 'close'
    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket('cm-engineers')
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, col]))
    pstr = bucket.Object(path).get().get('Body').read()
    str = "abcdefghijklmnop"*int(len(pstr)/16.0)
    return str


def thread_reader(event, context):
    sec_name = event
    category = 'DAILY'
    contract_name = None
    col = 'close'
    local = threading.local()
    bucket = getattr(local, 'bucket', None)
    if bucket is None:
        bucket = boto3.session.Session().resource('s3').Bucket('cm-engineers')
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, col]))
    pstr = bucket.Object(path).get().get('Body').read()
    str = "abcdefghijklmnop"*int(len(pstr)/16.0)
    return str


BUCKET = None

def global_reader(event, context):
    global BUCKET
    if BUCKET is None:
        BUCKET = boto3.session.Session().resource('s3').Bucket('cm-engineers')
    sec_name = event
    category = 'DAILY'
    contract_name = None
    col = 'close'
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, col]))
    pstr = BUCKET.Object(path).get().get('Body').read()
    str = "abcdefghijklmnop"*int(len(pstr)/16.0)
    return str


if __name__ == '__main__':
    print reader(1, None)
