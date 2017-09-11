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
# - Maksim Gubin, <maksim.gubin@cern.ch>, 2016
# - Mikhail Titov, <mikhail.titov@cern.ch>, 2017
#

import os

from ....utils import ConfigBase


config = ConfigBase('Config for two sources: DEfT, JEDI')
config.sqoop = ConfigBase()

# data source #0 (DEfT)

deft_src = ConfigBase('deft')  # name is used in parquet-converter
deft_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'TASKID=Long'),
    ('--map-column-java', 'START_TIME=Long'),
    ('--map-column-java', 'ENDTIME=Long'),
    ('--map-column-java', 'SUBMIT_TIME=Long'),
    ('--map-column-java', 'TIMESTAMP=Long')]

deft_src.db = ConfigBase()
deft_src.db.jdbc = 'jdbc:oracle:thin:@//ADCR2-DG-S.cern.ch:10121/ADCR.cern.ch'
deft_src.db.user = os.environ['PRODSYSPA_DEFT_USER']
deft_src.db.passphrase = os.environ['PRODSYSPA_DEFT_PASS']

deft_src.query = ConfigBase()
deft_src.query.select_columns = ['*']
deft_src.query.table = 't_production_task'
deft_src.query.conditions = [
    "(project like 'data%' OR project like 'mc%')"]
deft_src.query.time_range_column = 'start_time'

# data source #1 (JEDI)

jedi_src = ConfigBase('jedi')  # name is used in parquet-converter
jedi_src.options = [
    '--as-avrodatafile', ('-m', '1'),
    ('--map-column-java', 'JEDITASKID=Long')]

jedi_src.db = ConfigBase()
jedi_src.db.jdbc = 'jdbc:oracle:thin:@//ADCR2-DG-S.cern.ch:10121/ADCR.cern.ch'
jedi_src.db.user = os.environ['PRODSYSPA_JEDI_USER']
jedi_src.db.passphrase = os.environ['PRODSYSPA_JEDI_PASS']

jedi_src.query = ConfigBase()
jedi_src.query.select_columns = [
    'JEDITASKID',
    'GSHARE',
    'STATUS',
    'USERNAME',
    'REQID',
    'CLOUD',
    'SITE',
    'STARTTIME',
    'ENDTIME',
    'PRODSOURCELABEL',
    'WORKINGGROUP',
    'CORECOUNT',
    'PROCESSINGTYPE',
    'TASKPRIORITY',
    'ARCHITECTURE',
    'TRANSUSES',
    'TRANSHOME',
    'TRANSPATH',
    'SPLITRULE',
    'WORKQUEUE_ID',
    'ERRORDIALOG',
    'PARENT_TID',
    'EVENTSERVICE',
    'TICKETID',
    'TICKETSYSTEMTYPE',
    'STATECHANGETIME',
    'SUPERSTATUS',
    'CAMPAIGN',
    'GOAL',
    'NUCLEUS',
    'RAMCOUNT',
    'RAMUNIT',
    'WALLTIME',
    'WALLTIMEUNIT']
jedi_src.query.table = 'jedi_tasks'
jedi_src.query.conditions = [
    "(taskname like 'data%' OR taskname like 'mc%')",
    "endtime is not null"]
jedi_src.query.time_range_column = 'starttime'

# sqoop sources

config.sqoop.src0 = deft_src
config.sqoop.src1 = jedi_src

# pig options

config.pig = ConfigBase()
config.pig.options = [
    ('-f', '{0}/parquet-converter-default.pig'.
     format(os.path.dirname(os.path.abspath(__file__))))]
