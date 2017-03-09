#!/usr/bin/env python

"""
delete-versions.py

This script deletes all object versions recursively under a prefix.

Usage:
  delete-versions.py [--confirm] [--quiet] --bucket BUCKET --prefix PATH
  delete-versions.py (-h | --help)

Options:
  --bucket BUCKET           The S3 bucket.
  --prefix PREFIX           The key prefix under which all object versions will be deleted recursively.
  --confirm                 In the absence of this option, the script will only list what it would have deleted.
  --quiet                   Suppress any logging

  -h --help                 Display this message

Example:
    python delete-versions.py --confirm s3://cm-slycz/v0/gzip-rw-0.0.1/strategy-slm-v1
"""

from __future__ import absolute_import
from __future__ import print_function

import sys

import boto3

import envopt


def __main():
    env_prefix = 'DV'
    args = envopt.envopt(__doc__, env_prefix=env_prefix)
    bucket = args['--bucket']
    prefix = args['--prefix']
    confirmed = args['--confirm']
    quiet = args['--quiet']

    if not confirmed:
        print("NOTE: Just listing entries, not deleting. Add --confirm option to delete")

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_object_versions')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        deletions = list()
        for info in page['Versions']:
            version = info['VersionId']
            key = info['Key']
            deletions.append(dict(Key=key, VersionId=version))
            if not confirmed and not quiet:
                print("%s?%s" % (key, version))

        if confirmed:
            response = s3.delete_objects(Bucket=bucket, Delete=dict(Objects=deletions))
            deleted = response.get('Deleted', None)
            errors = response.get('Errors', None)
            if deleted and len(deleted) and not quiet:
                for d in deleted:
                    if d.get('DeleteMarker', None):
                        print("%s (delete marker)" % (d['Key']))
                    else:
                        print("%s?%s" % (d['Key'], d['VersionId']))
            if errors and len(errors):
                for d in errors:
                    print("FAILED: %s?%s (%s: %s)" % (d['Key'], d['VersionId'], d['Code'], d['Message']))
                sys.exit(1)


if __name__ == '__main__':
    __main()
