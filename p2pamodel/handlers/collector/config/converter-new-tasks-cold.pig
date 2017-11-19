REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

REGISTER '/afs/cern.ch/user/m/matitov/pig/lib/elephant-bird-*.jar';
DEFINE decode_json com.twitter.elephantbird.pig.piggybank.JsonStringToMap();

deft_tasks_initial = LOAD '$deft_tasks' USING AvroStorage();
deft_tasks = FOREACH deft_tasks_initial GENERATE TASKID, SUBMIT_TIME, TIMESTAMP, decode_json(JEDI_TASK_PARAMETERS) AS TASK_PARAMETERS;

prepared = FOREACH deft_tasks GENERATE
TASKID,
SUBMIT_TIME AS SUBMITTIME,
(TIMESTAMP - SUBMIT_TIME) AS DURATION,
REGEX_EXTRACT(TASK_PARAMETERS#'taskName', '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(TASK_PARAMETERS#'taskName', '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
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

store prepared into '$out' using parquet.pig.ParquetStorer;
