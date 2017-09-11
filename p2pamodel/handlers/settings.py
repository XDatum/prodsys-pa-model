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

from datetime import datetime as dt

from ..utils import EnumTypes

HDFS_BASE_DIR = '/atlas/prodsys_model'

HDFS_DATA_DIR = '{0}/data'.format(HDFS_BASE_DIR)
HDFS_PRIVATE_DIR = '{0}/private'.format(HDFS_BASE_DIR)

STORAGE_PATH_FORMAT = 'hdfs://{0}/{1}'.format(HDFS_DATA_DIR, '{work_dir}')

WORK_DIR_NAME_DEFAULT = dt.utcnow().strftime('%Y%m%d')

DataType = EnumTypes(
    ('Training', 'training'),
    ('Test', 'test'),
    ('Model', 'model'),
    ('Eval', 'eval'),
    ('Input', 'input')
)


try:
    from .settings_local import *
except ImportError:
    pass
