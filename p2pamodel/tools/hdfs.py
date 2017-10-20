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

import os
import time

from ..utils import pyCMD


def create_private_file(dir_name, service_name, message):
    """
    Create private file at HDFS storage.

    @param dir_name: HDFS base directory.
    @type dir_name: str
    @param service_name: Service name.
    @type service_name: str
    @param message: Private message.
    @type message: str
    @return: Full file path.
    @rtype: str
    """
    file_name = '{0}{1}.sqoop'.format(service_name, int(time.time()))
    file_path = '{0}/{1}'.format(dir_name, file_name)

    with open(file_name, 'w') as f:
        f.write(message)

    pyCMD('hdfs', ['dfs', '-put', file_name, file_path]).execute()
    pyCMD('hdfs', ['dfs', '-chmod', '400', file_path]).execute()

    os.remove(file_name)
    return file_path


def remove_file(path):
    """
    Remove file from HDFS storage.

    @param path: Full file path.
    @type path: str
    """
    pyCMD('hdfs', ['dfs', '-rm', '-skipTrash', path]).execute()


def remove_dir(path):
    """
    Remove directory from HDFS storage.

    @param path: Full dir path.
    @type path: str
    """
    pyCMD('hdfs', ['dfs', '-rm', '-r', '-f', '-skipTrash', path]).execute()
