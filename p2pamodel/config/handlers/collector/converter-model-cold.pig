REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/afs/cern.ch/user/m/matitov/pig/lib/elephant-bird-*.jar';
DEFINE decode_json com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

deft0_tasks_v1 = LOAD '$deft0_tasks' USING AvroStorage();
deft0_tasks_v2 = FOREACH deft0_tasks_v1 GENERATE TASKID, decode_json(JEDI_TASK_PARAMETERS) AS TASK_PARAMETERS;
deft0_tasks = FOREACH deft0_tasks_v2 GENERATE
TASKID,
TASK_PARAMETERS#'userName' AS USERNAME,
TASK_PARAMETERS#'workingGroup' AS WORKINGGROUP,
TASK_PARAMETERS#'prodSourceLabel' AS PRODSOURCELABEL,
TASK_PARAMETERS#'processingType' AS PROCESSINGTYPE,
TASK_PARAMETERS#'architecture' AS ARCHITECTURE,
TASK_PARAMETERS#'transPath' AS TRANSPATH,
TASK_PARAMETERS#'transUses' AS TRANSUSES,
TASK_PARAMETERS#'coreCount' AS CORECOUNT,
TASK_PARAMETERS#'ramCount' AS RAMCOUNT,
TASK_PARAMETERS#'ramUnit' AS RAMUNIT,
TASK_PARAMETERS#'taskPriority' AS PRIORITY;

deft1_tasks = LOAD '$deft1_tasks' USING AvroStorage();
jedi_tasks = LOAD '$jedi_tasks' USING AvroStorage();

joint = JOIN deft0_tasks BY TASKID, deft1_tasks BY TASKID, jedi_tasks BY JEDITASKID;

prepared = FOREACH joint GENERATE
deft0_tasks::TASKID AS TASKID,
(jedi_tasks::ENDTIME - jedi_tasks::STARTTIME) AS DURATION,
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
deft0_tasks::USERNAME AS USERNAME,
deft0_tasks::WORKINGGROUP AS WORKINGGROUP,
deft0_tasks::PRODSOURCELABEL AS PRODSOURCELABEL,
deft0_tasks::PROCESSINGTYPE AS PROCESSINGTYPE,
deft0_tasks::ARCHITECTURE AS ARCHITECTURE,
deft0_tasks::TRANSPATH AS TRANSPATH,
deft0_tasks::TRANSUSES AS TRANSUSES,
deft0_tasks::CORECOUNT AS CORECOUNT,
deft0_tasks::RAMCOUNT AS RAMCOUNT,
deft0_tasks::RAMUNIT AS RAMUNIT,
deft0_tasks::PRIORITY AS PRIORITY,
deft1_tasks::TOTAL_REQ_EVENTS AS TOTALREQEVENTS;

STORE prepared INTO '$out' USING parquet.pig.ParquetStorer;
