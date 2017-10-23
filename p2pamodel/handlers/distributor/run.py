#!/usr/bin/env spark-submit
#
# Copyright European Organization for Nuclear Research (CERN),
#           National Research Centre "Kurchatov Institute" (NRC KI)
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017
#

import argparse
import sys

from p2pamodel.handlers.distributor.client import Distributor


def get_args():
    """
    Get arguments.

    @return: Arguments namespace.
    @rtype: _AttributeHolder
    """

    parser = argparse.ArgumentParser(
        description='Distribute processed/collected data.'
    )

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '--data-dir',
        dest='data_dir',
        help='Relative path of the directory with source data.',
        required=False
    )

    parser.add_argument(
        '--method',
        dest='method',
        type=str,
        help='Method name to use for data processing.',
        required=True
    )

    return parser.parse_args(sys.argv[1:])


def proceed(args):
    """
    Proceed component execution.

    @param args: Arguments.
    @type args: _AttributeHolder
    """

    distributor = Distributor(data_dir=args.data_dir,
                              verbose=args.verbose)

    if args.method == 'set_ttcr_dict':
        distributor.set_ttcr_dict()
    elif args.method == 'set_ttcj_timestamp':
        distributor.set_ttcj_timestamp()


if __name__ == '__main__':
    proceed(args=get_args())
