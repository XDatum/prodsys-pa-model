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

from ..configbase import ConfigBase


api_credentials = ConfigBase('Config for distributor module')
api_credentials.user = os.environ['P2PA_PROVIDER_USER']
api_credentials.passphrase = os.environ['P2PA_PROVIDER_PASS']
api_credentials.url = os.environ['P2PA_PROVIDER_URL']
