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
(TIMESTAMP - SUBMIT_TIME) AS DURATION,
REGEX_EXTRACT(TASK_PARAMETERS#'TASKNAME', '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(TASK_PARAMETERS#'TASKNAME', '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
TASK_PARAMETERS#'USERNAME' AS USERNAME,
TASK_PARAMETERS#'WORKINGGROUP' AS WORKINGGROUP,
TASK_PARAMETERS#'PRODSOURCELABEL' AS PRODSOURCELABEL,
TASK_PARAMETERS#'PROCESSINGTYPE' AS PROCESSINGTYPE,
TASK_PARAMETERS#'ARCHITECTURE' AS ARCHITECTURE,
TASK_PARAMETERS#'TRANSPATH' AS TRANSPATH,
TASK_PARAMETERS#'TRANSUSES' AS TRANSUSES,
TASK_PARAMETERS#'CLOUD' AS CLOUD;

store prepared into '$out' using parquet.pig.ParquetStorer;
