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

DAYS_DEFAULT = 90
DAYS_OFFSET_DEFAULT = 0

TASK_STATE_SQL_MAPPING = {
    'new': "status in ('registered', 'assigning')",
    'in_progress': "status in ('submitting', 'running')",
    'finished': "status in ('done', 'finished')"
}
