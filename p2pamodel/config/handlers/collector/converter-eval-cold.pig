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
deft_tasks::TASKID AS TASKID,
(jedi_tasks::ENDTIME - jedi_tasks::STARTTIME) AS DURATION,
(deft_tasks::TTCR_TIMESTAMP - deft_tasks::SUBMIT_TIME) AS DURATIONTTCR,
(deft_tasks::TTCJ_TIMESTAMP - deft_tasks::SUBMIT_TIME) AS DURATIONTTCJ,
REGEX_EXTRACT(deft_tasks::TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(deft_tasks::TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP;

reduceddata = FOREACH prepared GENERATE
(chararray) $0 as TASKID,
(chararray) $1 as DURATION,
(chararray) $2 as DURATIONTTCR,
(chararray) $3 as DURATIONTTCJ,
(chararray) $4 as PROJECT,
(chararray) $5 as PRODUCTIONSTEP;

%declare prefix '_plain';
reduceddata_ordered_by_taskid = ORDER reduceddata BY TASKID ASC;
STORE reduceddata_ordered_by_taskid INTO '$out$prefix';
