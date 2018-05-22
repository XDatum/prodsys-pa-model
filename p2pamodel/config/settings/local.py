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

from .base import *

HDFS_BASE_DIR = '/user/matitov'

HDFS_DATA_DIR = HDFS_DATA_DIR_FORMAT.format(hdfs_dir_name=HDFS_BASE_DIR)
HDFS_PRIVATE_DIR = HDFS_PRIVATE_DIR_FORMAT.format(hdfs_dir_name=HDFS_BASE_DIR)

STORAGE_PATH_FORMAT = 'hdfs://{0}/{1}'.format(HDFS_DATA_DIR, '{dir_name}')
