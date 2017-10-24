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

from ....utils import ConfigBase


config = ConfigBase('Config for one source: DEfT')
config.sqoop = ConfigBase()

# data source #0 (DEfT)

deft_src = ConfigBase('deft')  # name is used in parquet-converter
deft_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'TASKID=Long,JEDI_TASK_PARAMETERS=String'),
    ('--inline-lob-limit', '0')
]

deft_src.db = ConfigBase()
deft_src.db.jdbc = 'jdbc:oracle:thin:@//ADCR2-DG-S.cern.ch:10121/ADCR.cern.ch'
deft_src.db.user = os.environ['P2PA_SRC_DEFT_USER']
deft_src.db.passphrase = os.environ['P2PA_SRC_DEFT_PASS']

deft_src.query = ConfigBase()
deft_src.query.select_columns = [
    'TASKID',
    'JEDI_TASK_PARAMETERS',
    'SUBMIT_TIME',
    'TIMESTAMP'
]
deft_src.query.table = 't_task'
deft_src.query.conditions = [
    "(taskname like 'data%' OR taskname like 'mc%')"
]
deft_src.query.time_range_column = 'submit_time'

# sqoop source(s)

config.sqoop.src0 = deft_src

# pig options

config.pig = ConfigBase()
config.pig.options = [
    ('-f', '{0}/converter-new-tasks-cold.pig'.
     format(os.path.dirname(os.path.abspath(__file__))))]
