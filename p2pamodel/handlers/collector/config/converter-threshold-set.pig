REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

deft_tasks = LOAD '$deft_tasks' USING AvroStorage();
jedi_tasks = LOAD '$jedi_tasks' USING AvroStorage();

joint = JOIN deft_tasks BY TASKID, jedi_tasks BY JEDITASKID;

prepared = FOREACH joint GENERATE
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(jedi_tasks::TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
deft_tasks::PROVENANCE AS PROVENANCE,
(jedi_tasks::ENDTIME - jedi_tasks::STARTTIME) AS DURATION;

store prepared into '$out' using parquet.pig.ParquetStorer;
