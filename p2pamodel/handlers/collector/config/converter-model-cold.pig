REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/afs/cern.ch/user/m/matitov/pig/lib/elephant-bird-*.jar';
DEFINE decode_json com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

deft_tasks_v1 = LOAD '$deft_tasks' USING AvroStorage();
deft_tasks_v2 = FOREACH deft_tasks_v1 GENERATE TASKID, SUBMIT_TIME, decode_json(JEDI_TASK_PARAMETERS) AS TASK_PARAMETERS;
deft_tasks = FOREACH deft_tasks_v2 GENERATE
TASKID,
TASK_PARAMETERS#'userName' AS USERNAME,
TASK_PARAMETERS#'workingGroup' AS WORKINGGROUP,
TASK_PARAMETERS#'prodSourceLabel' AS PRODSOURCELABEL,
TASK_PARAMETERS#'processingType' AS PROCESSINGTYPE,
TASK_PARAMETERS#'architecture' AS ARCHITECTURE,
TASK_PARAMETERS#'transPath' AS TRANSPATH,
TASK_PARAMETERS#'transUses' AS TRANSUSES,
TASK_PARAMETERS#'cloud' AS CLOUD,
TASK_PARAMETERS#'ramUnit' AS RAMUNIT,
TASK_PARAMETERS#'ramCount' AS RAMCOUNT,
TASK_PARAMETERS#'coreCount' AS CORECOUNT,
((DaysBetween(ToDate(SUBMIT_TIME),ToDate(0L)) + 4L) % 7) as WEEKDAY;

jedi_tasks = LOAD '$jedi_tasks' USING AvroStorage();

joint = JOIN deft_tasks BY TASKID, jedi_tasks BY JEDITASKID;

prepared = FOREACH joint GENERATE
deft_tasks::TASKID AS TASKID,
(jedi_tasks::ENDTIME - jedi_tasks::STARTTIME) AS DURATION,
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
deft_tasks::USERNAME AS USERNAME,
deft_tasks::WORKINGGROUP AS WORKINGGROUP,
deft_tasks::PRODSOURCELABEL AS PRODSOURCELABEL,
deft_tasks::PROCESSINGTYPE AS PROCESSINGTYPE,
deft_tasks::ARCHITECTURE AS ARCHITECTURE,
deft_tasks::TRANSPATH AS TRANSPATH,
deft_tasks::TRANSUSES AS TRANSUSES,
deft_tasks::CLOUD AS CLOUD,
deft_tasks::RAMUNIT AS RAMUNIT,
deft_tasks::RAMCOUNT AS RAMCOUNT,
deft_tasks::CORECOUNT AS CORECOUNT,
deft_tasks::WEEKDAY AS WEEKDAY;

STORE prepared INTO '$out' USING parquet.pig.ParquetStorer;
