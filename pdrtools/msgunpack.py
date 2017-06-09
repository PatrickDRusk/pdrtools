#!/usr/bin/env python

"""
msgunpack

Reads an S3 blob, runs msgunpack on it, and prints the result.

Usage:
    msgunpack.py [options] <target>...
    msgunpack.py (-h | --help)

Options:
    --bucket BUCKET             The S3 bucket [default: cm-engineers]
    --prefix PREFIX             A common prefix for the targets [default: pdr/blobz]

    -h --help                   Display this message

Example:
    msgunpack.py --bucket cm-engineers --prefix pdr/blobz AA_COMDTY/DAILY/close
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import boto3
import envopt
import msgpack


def __main():
    env_prefix = 'MU'
    args = envopt.envopt(__doc__, env_prefix=env_prefix)

    bucket = args['--bucket']
    prefix = args['--prefix']
    targets = args['<target>']

    s3_bucket = boto3.resource('s3').Bucket(bucket)

    for target in targets:
        print_blob(s3_bucket, prefix, target)


def print_blob(s3_bucket, prefix, target):
    path = os.path.join(*filter(None, [prefix, target]))
    mstr = s3_bucket.Object(path).get().get('Body').read()
    data = msgpack.unpackb(mstr, use_list=False)
    print(data)


if __name__ == '__main__':
    __main()
