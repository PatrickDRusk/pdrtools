#! /usr/bin/env python

"""
Lambda functions for the slim_price_data_reader.

To deploy:

  zip /tmp/slim_price_data_reader.zip slim_price_data_reader.py

  aws lambda create-function --function-name pdr_price_data --runtime python2.7 \
    --role arn:aws:iam::528461152743:role/TradingOpsLambda --handler slim_price_data_reader.main \
    --zip-file fileb:///tmp/slim_price_data_reader.zip --publish

To delete the functions:

  for name in string_upcase string_reverse string_concat; do
    aws lambda delete-function --function-name $name
  done

"""

import cPickle
import os
import time

import boto3

current_millis = lambda: int(round(time.time() * 1000))

COMDTYS = (
    "CO_COMDTY",
)

MAQUILAS = (
    "CO_COMDTY:FRONT:V1",
)

BUCKET = None
PREFIX = 'pdr/blobz'

DO_WRITES = False
WRITE_CONTRACT_DATA = True

RETURN_STRS = []


def read_blob(sec_name, contract_name, category, blob_name):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, blob_name]))
    pstr = BUCKET.Object(path).get().get('Body').read()
    data = cPickle.loads(pstr)
    return data


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    RETURN_STRS.append(pattern + str(delta))
    return current_millis()


def read_symbol(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        # noinspection PyUnusedLocal
        contract_df = read_blob(sec_name, contract_name, "DAILY", 'close')

    log_millis(millis, "Time to read all %s contract prices: " % sec_name)


def read_symbol_big(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        # noinspection PyUnusedLocal
        contract_df = read_blob(sec_name, contract_name, "DAILY", 'all')

    log_millis(millis, "Time to read all %s contract prices (big): " % sec_name)


def read_maquila(sec_name, contract_name=None, col='return'):
    millis = current_millis()
    data = read_blob(sec_name, contract_name, "DAILY", col)
    log_millis(millis, "Time to read %s returns: " % sec_name)
    return data


def main():
    global BUCKET
    s3 = boto3.resource('s3')
    BUCKET = s3.Bucket('cm-engineers')

    for sec_name in COMDTYS:
        read_blob(sec_name, None, None, 'metadata')   # to prime the timings
        read_maquila(sec_name, "0000", col='all')
        read_symbol(sec_name)
        read_symbol_big(sec_name)

    for maquila_key in MAQUILAS:
        read_maquila(maquila_key)

    return RETURN_STRS


if __name__ == '__main__':
    main()
