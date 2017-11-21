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


config = ConfigBase('Config for two sources: DEfT x 2')
config.sqoop = ConfigBase()

# data source #0 (DEfT/t_task)

deft0_src = ConfigBase('deft0')  # name is used in parquet-converter
deft0_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'TASKID=Long,JEDI_TASK_PARAMETERS=String'),
    ('--inline-lob-limit', '0')
]

deft0_src.db = ConfigBase()
deft0_src.db.jdbc = os.environ['P2PA_SRC_DEFT_JDBC']
deft0_src.db.user = os.environ['P2PA_SRC_DEFT_USER']
deft0_src.db.passphrase = os.environ['P2PA_SRC_DEFT_PASS']

deft0_src.query = ConfigBase()
deft0_src.query.select_columns = [
    'TASKID',
    'JEDI_TASK_PARAMETERS',
    'SUBMIT_TIME',
    'TIMESTAMP'
]
deft0_src.query.table = 't_task'
deft0_src.query.conditions = [
    "(taskname like 'data%' OR taskname like 'mc%')"
]
deft0_src.query.time_range_column = 'submit_time'

# data source #1 (DEfT/t_production_task)

deft1_src = ConfigBase('deft1')  # name is used in parquet-converter
deft1_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'TASKID=Long')
]

deft1_src.db = ConfigBase()
deft1_src.db.jdbc = os.environ['P2PA_SRC_DEFT_JDBC']
deft1_src.db.user = os.environ['P2PA_SRC_DEFT_USER']
deft1_src.db.passphrase = os.environ['P2PA_SRC_DEFT_PASS']

deft1_src.query = ConfigBase()
deft1_src.query.select_columns = [
    'TASKID',
    'TOTAL_REQ_EVENTS'
]
deft1_src.query.table = 't_production_task'
deft1_src.query.conditions = [
    "(taskname like 'data%' OR taskname like 'mc%')"
]
deft1_src.query.time_range_column = 'submit_time'

# sqoop source(s)

config.sqoop.src0 = deft0_src
config.sqoop.src1 = deft1_src

# pig options

config.pig = ConfigBase()
config.pig.options = [
    ('-f', '{0}/converter-new-tasks-cold.pig'.
     format(os.path.dirname(os.path.abspath(__file__))))]
