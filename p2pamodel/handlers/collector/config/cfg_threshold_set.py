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

COMMON_QUERY_CONDITIONS = [
    "(taskname like 'data%' OR taskname like 'mc%')",
    "status in ('done', 'finished')"
]


# collector configuration

config = ConfigBase('Config for two sources: DEfT, JEDI')
config.sqoop = ConfigBase()


# data source #0 (DEfT/t_production_task)

deft_src = ConfigBase('deft')  # name is used in parquet-converter
deft_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'TASKID=Long')
]

deft_src.db = ConfigBase()
deft_src.db.jdbc = os.environ['P2PA_SRC_DEFT_JDBC']
deft_src.db.user = os.environ['P2PA_SRC_DEFT_USER']
deft_src.db.passphrase = os.environ['P2PA_SRC_DEFT_PASS']

deft_src.query = ConfigBase()
deft_src.query.select_columns = [
    'TASKID',
    'TASKNAME',
    'PROVENANCE'
]
deft_src.query.table = 't_production_task'
deft_src.query.conditions = COMMON_QUERY_CONDITIONS
deft_src.query.time_range_column = 'start_time'


# data source #1 (JEDI/jedi_tasks)

jedi_src = ConfigBase('jedi')  # name is used in parquet-converter
jedi_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'JEDITASKID=Long')
]

jedi_src.db = ConfigBase()
jedi_src.db.jdbc = os.environ['P2PA_SRC_JEDI_JDBC']
jedi_src.db.user = os.environ['P2PA_SRC_JEDI_USER']
jedi_src.db.passphrase = os.environ['P2PA_SRC_JEDI_PASS']

jedi_src.query = ConfigBase()
jedi_src.query.select_columns = [
    'JEDITASKID',
    'STARTTIME',
    'ENDTIME',
    'TASKNAME',
    'WORKINGGROUP'
]
jedi_src.query.table = 'jedi_tasks'
jedi_src.query.conditions = COMMON_QUERY_CONDITIONS + [
    "endtime is not null"
]
jedi_src.query.time_range_column = 'starttime'


# sqoop sources

config.sqoop.src0 = deft_src
config.sqoop.src1 = jedi_src


# pig options

config.pig = ConfigBase()
config.pig.options = [
    ('-f', '{0}/converter-threshold-set.pig'.
     format(os.path.dirname(os.path.abspath(__file__))))]
