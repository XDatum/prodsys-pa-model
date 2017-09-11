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

from p2pamodel.handlers.predictor.client import Predictor


def get_args():
    """
    Get arguments.

    @return: Arguments namespace.
    @rtype: _AttributeHolder
    """

    parser = argparse.ArgumentParser(
        description='Make predictions based on trained/tested model.'
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--train', action='store_true')
    group.add_argument('--evaluate', action='store_true')

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
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
        '--with-eval',
        dest='with_eval',
        action='store_true',
        default=False
    )

    parser.add_argument(
        '--test-data-dir',
        dest='test_data_dir',
        help='Relative path of the directory with test data.',
        required=False
    )

    parser.add_argument(
        '--input-data-dir',
        dest='input_data_dir',
        help='Relative path of the directory with input data.',
        required=False
    )

    return parser.parse_args(sys.argv[1:])


def proceed(args):
    """
    Proceed component execution.

    @param args: Arguments.
    @type args: _AttributeHolder
    """

    predictor = Predictor(work_dir=args.work_dir,
                          test_data_dir=args.test_data_dir,
                          input_data_dir=args.input_data_dir,
                          verbose=args.verbose)

    if args.train:
        predictor.run_trainer(with_eval=args.with_eval)
    elif args.evaluate:
        predictor.run_evaluator()
    else:
        predictor.run_predictor()


if __name__ == '__main__':
    proceed(args=get_args())
