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

from datetime import datetime as dt

CONFIG_PKG_PATH_FORMAT = 'p2pamodel.config.handlers.{component}'

HDFS_DATA_DIR_FORMAT = '{hdfs_dir_name}/data'
HDFS_PRIVATE_DIR_FORMAT = '{hdfs_dir_name}/private'

SERVICE_NAME = 'ProdSys2-PA'

WORK_DIR_NAME_DEFAULT = dt.utcnow().strftime('%Y%m%d')
