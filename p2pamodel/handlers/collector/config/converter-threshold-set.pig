REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

jedi_tasks = LOAD '$jedi_tasks' USING AvroStorage();

prepared = FOREACH jedi_tasks GENERATE
REGEX_EXTRACT(TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
WORKINGGROUP AS WORKINGGROUP,
(ENDTIME - STARTTIME) AS DURATION;

STORE prepared INTO '$out' USING parquet.pig.ParquetStorer;
