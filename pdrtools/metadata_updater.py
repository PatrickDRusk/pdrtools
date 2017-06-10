#! /usr/bin/env python

"""
metadata_updater is a script to view and update price data store global metadata.

Usage:
  metadata_updater.py [options] [<security> [<key> [<value>]]]

The security can be None to get/set the global defaults.

If value is specified, it will set the appropriate attribute.  In the absence, it gets the attribute(s).
value is interpreted in the following ways:
  * if integral, as an integer
  * if either of the strings "True" or "False", as True or False
  * if the string "None", as None
  * if colon-separated, as a tuple of values, interpreted as above
  * otherwise as a string

Options:
  --bucket BUCKET         The S3 bucket of the price data store [default: cm-engineers]
  --prefix PREFIX         The prefix for entries in the data store [default: pdr/blobz]

  --confirm               Required to actually apply modifications

Examples:
  metadata_updater.py   # to see all the metadata (very large!)
  metadata_updater.py CL_COMDTY   # to see the metadata specific to CL_COMDTY
  metadata_updater.py CL_COMDTY blacklist   # to see the blacklist attribute for CL_COMDTY
  metadata_updater.py CL_COMDTY blacklist False   # to set the blacklist attribute for CL_COMDTY to False
  metadata_updater.py CL_COMDTY current_through None   # to remove the current_through attribute for CL_COMDTY
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import traceback

import boto3
import msgpack

from envopt import envopt

from price_data import storage as price


# Global to hold the global metadata for the price store
METADATA = None  # type: price.Metadata

# These globals get set by the processing of command-line arguments

BUCKET = None
PREFIX = None

DO_WRITES = False


def __main():
    args = envopt(__doc__, env_prefix='MDU')

    security, key, value = process_args(args)

    load_metadata()

    do_work(security, key, value)

    if value is not None:
        save_metadata()


def process_args(args):
    global BUCKET, PREFIX, DO_WRITES

    bucket = args['--bucket']
    BUCKET = boto3.resource('s3').Bucket(bucket)
    PREFIX = args['--prefix']
    print("Using datastore under s3://%s/%s" % (bucket, PREFIX))

    security = args['<security>']
    key = args['<key>']
    value = args['<value>']

    DO_WRITES = args['--confirm']
    if (not DO_WRITES) and (value is not None):
        print("DRY RUN!  Skipping all writing. Use --confirm to apply any changes.")

    return security, key, value


def load_metadata():
    global METADATA

    # noinspection PyBroadException
    try:
        # Initialize contract map with what we've seen before
        METADATA = price.Metadata(read_blob(None, None, price.PriceData.METADATA))
    except Exception:
        print("NO METADATA FOUND!")
        traceback.print_exc()
        sys.exit(1)


def save_metadata():
    write_blob(None, None, price.PriceData.METADATA, METADATA)


def do_work(security, key, value):
    if value is not None:
        val = interpret_value(value)
        sec = interpret_value(security)
        # noinspection PyProtectedMember
        METADATA._get_set_attr(key, sec, val)
        # dict_ = METADATA if sec is None else METADATA['securities'][sec]
        # if val is None:
        #     del dict_[key]
        # else:
        #     dict_[key] = val

    elif key is not None:
        sec = interpret_value(security)
        print(METADATA._get_set_attr(key, sec))

    elif security is not None:
        sec = interpret_value(security)
        if sec is None:
            dict_ = dict(METADATA)
            del dict_['contract_map']
            del dict_['securities']
        else:
            dict_ = METADATA['securities'].get(sec, dict())
        print(dict_)
    else:
        print(METADATA)


def interpret_value(value):
    if value is None:
        return None

    if ':' in value:
        return tuple(interpret_value(x) for x in value.split(':'))

    try:
        return int(value)
    except ValueError:
        pass

    if value == 'True':
        return True
    elif value == 'False':
        return False
    elif value == 'None':
        return None
    else:
        return value


def write_blob(sec_name, category, blob_name, data):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, blob_name]))
    if DO_WRITES:
        mstr = msgpack.packb(data, use_bin_type=True)
        BUCKET.put_object(Key=path, Body=mstr)
    else:
        pass


def read_blob(sec_name, category, blob_name):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, blob_name]))
    mstr = BUCKET.Object(path).get().get('Body').read()
    return msgpack.unpackb(mstr, use_list=False)


if __name__ == '__main__':
    __main()
