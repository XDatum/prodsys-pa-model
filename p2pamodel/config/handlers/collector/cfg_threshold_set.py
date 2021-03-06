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

import os

from ..configbase import ConfigBase

COMMON_QUERY_CONDITIONS = [
    "(taskname like 'data%' OR taskname like 'mc%')",
    "status in ('done', 'finished')"
]


# collector configuration

config = ConfigBase('Config for one source: JEDI')
config.sqoop = ConfigBase()


# data source #0 (JEDI/jedi_tasks)

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
    "starttime is not null",
    "endtime is not null"
]
jedi_src.query.time_range_column = 'starttime'


# sqoop sources

config.sqoop.src0 = jedi_src


# pig options

config.pig = ConfigBase()
config.pig.options = [
    ('-f', '{0}/converter-threshold-set.pig'.
     format(os.path.dirname(os.path.abspath(__file__))))]
