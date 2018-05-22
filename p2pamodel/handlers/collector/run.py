#!/usr/bin/env python
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
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017-2018
#

import argparse
import sys

from p2pamodel.handlers.collector.client import (Collector,
                                                 DataType)

DAYS_DEFAULT = 90
DAYS_OFFSET_DEFAULT = 0


def get_args():
    """
    Get arguments.

    @return: Arguments namespace.
    @rtype: _AttributeHolder
    """

    parser = argparse.ArgumentParser(
        description='Collect task information from DEfT/JEDI.'
    )

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '-f', '--force',
        dest='force',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '-w', '--work-dir',
        dest='work_dir',
        help='Relative path of the working directory.',
        required=False
    )

    parser.add_argument(
        '-c', '--config',
        dest='config',
        type=str,
        help='Configuration module name (in config-sub-pkg).',
        required=False
    )

    parser.add_argument(
        '--days',
        dest='days',
        type=int,
        default=DAYS_DEFAULT,
        help='Number of days for the requested period.',
        required=False
    )

    parser.add_argument(
        '--days-offset',
        dest='days_offset',
        type=int,
        default=DAYS_OFFSET_DEFAULT,
        help='Number of days to offset.',
        required=False
    )

    parser.add_argument(
        '--output-type',
        dest='output_type',
        choices=DataType.attrs.values(),
        help='Type of the output data.',
        required=False
    )

    return parser.parse_args(sys.argv[1:])


def proceed(args):
    """
    Proceed component execution.

    @param args: Arguments.
    @type args: _AttributeHolder
    """

    collector = Collector(work_dir=args.work_dir,
                          config=args.config,
                          verbose=args.verbose)

    collector.execute(days=args.days,
                      days_offset=args.days_offset,
                      output_type=args.output_type,
                      force=args.force)


if __name__ == '__main__':
    proceed(args=get_args())
