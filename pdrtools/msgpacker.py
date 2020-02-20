#!/usr/bin/env python

"""
msgpacker

Reads from standard input, runs msgpack on it, and writes to an S3 blob.

Usage:
    msgpacker.py [options] <target>
    msgpacker.py (-h | --help)

Options:
    --bucket BUCKET             The S3 bucket [default: cm-engineers]
    --prefix PREFIX             A common prefix for the targets [default: pdr/blobz]
    --json                      Add if input should be processed by json.loads() first

    -h --help                   Display this message

Example:
    msgpacker.py --bucket cm-engineers --prefix pdr/blobz AA_COMDTY/DAILY/close
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import sys

import boto3
import envopt
import msgpack


def __main():
    env_prefix = 'MU'
    args = envopt.envopt(__doc__, env_prefix=env_prefix)

    bucket = args['--bucket']
    prefix = args['--prefix']
    target = args['<target>']
    is_json = args['--json']

    s3_bucket = boto3.resource('s3').Bucket(bucket)

    data = sys.stdin.read()
    if is_json:
        data = json.loads(data)

    write_blob(s3_bucket, prefix, target, data)


def write_blob(s3_bucket, prefix, target, data):
    path = os.path.join(*filter(None, [prefix, target]))
    bytes_ = msgpack.packb(data, use_bin_type=True)
    s3_bucket.Object(path).put(Body=bytes_)


if __name__ == '__main__':
    __main()
