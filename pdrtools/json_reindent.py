#! /usr/bin/env python

"""
json_reindent.py re-indents files containing json with a new indentation level.

Usage:
  json_reindent.py <file>... [--indent=<indent>]

Options:
  --indent INDENT       The number of spaces in the indent [default: 2]
"""

import json

from envopt import envopt


def __main():
    """
    Function to process arguments and call main logic when invoked from a command line.
    @return: None
    """
    args = envopt(__doc__, env_prefix='JIND')

    json_fnames = args['<file>']

    indent = int(args['--indent'])

    json_reindent(json_fnames, indent)


def json_reindent(json_fnames, indent=2):
    """
    Re-indent the given file with JSON code with a new indentation level.

    @param json_fname: the file to re-indent
    @param indent: the indentation level
    @return: None
    """
    for json_fname in json_fnames:
        with open(json_fname, mode='r') as json_file:
            contents = json.load(json_file)

        with open(json_fname, mode='w') as json_file:
            json_file.write(json.dumps(contents, indent=indent))


if __name__ == '__main__':
    __main()
