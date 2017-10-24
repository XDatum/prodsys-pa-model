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
import sys

from ...utils import ConfigBase

api_credentials = ConfigBase('Config for distributor module')
api_credentials.user = os.environ['P2PA_PROVIDER_USER']
api_credentials.passphrase = os.environ['P2PA_PROVIDER_PASS']

if not os.environ.get('SPARK_HOME'):
    os.environ['SPARK_HOME'] = '/usr/lib/spark'
    sys.path.append(os.environ['SPARK_HOME'])
    os.environ['PYSPARK_PYTHON'] = '/etc/spark/python'

SERVICE_NAME = 'ProdSysPA'
