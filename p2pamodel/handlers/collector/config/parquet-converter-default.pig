REGISTER '/usr/lib/pig/piggybank.jar' ;
REGISTER '/usr/lib/pig/lib/avro-*.jar';
REGISTER '/usr/lib/pig/lib/jackson-*.jar';
REGISTER '/usr/lib/pig/lib/json-*.jar';
REGISTER '/usr/lib/pig/lib/jython-*.jar';
REGISTER '/usr/lib/pig/lib/snappy-*.jar';

deft_tasks = load '$deft_tasks' using AvroStorage();
jedi_tasks = load '$jedi_tasks' using AvroStorage();

joint = join deft_tasks by TASKID, jedi_tasks by JEDITASKID;

prepared = foreach joint generate
deft_tasks::TASKID AS TASKID,
(jedi_tasks::ENDTIME - jedi_tasks::STARTTIME) AS DURATION,
jedi_tasks::STARTTIME as STARTTIME,
jedi_tasks::ENDTIME as ENDTIME,
REGEX_EXTRACT(deft_tasks::TASKNAME, '^(.*?)\\.',1) AS PROJECT,
REGEX_EXTRACT(deft_tasks::TASKNAME, '^(.*?\\.){3}(.*?)\\.',2) AS PRODUCTIONSTEP,
deft_tasks::PROVENANCE as PROVENANCE,
deft_tasks::TOTAL_EVENTS as TOTAL_EVENTS,
deft_tasks::TOTAL_REQ_EVENTS as TOTAL_REQ_EVENTS,
deft_tasks::TOTAL_REQ_JOBS as TOTAL_REQ_JOBS,
deft_tasks::TOTAL_DONE_JOBS as TOTAL_DONE_JOBS,
deft_tasks::NFILESTOBEUSED as NFILESTOBEUSED,
deft_tasks::PRIORITY as PRIORITY,
deft_tasks::CURRENT_PRIORITY as CURRENT_PRIORITY,
jedi_tasks::STATUS AS STATUS,
jedi_tasks::USERNAME AS USERNAME,
jedi_tasks::REQID AS REQID,
jedi_tasks::CLOUD AS CLOUD,
jedi_tasks::SITE AS SITE,
jedi_tasks::PRODSOURCELABEL AS PRODSOURCELABEL,
jedi_tasks::WORKINGGROUP AS WORKINGGROUP,
jedi_tasks::CORECOUNT AS CORECOUNT,
jedi_tasks::PROCESSINGTYPE AS PROCESSINGTYPE,
jedi_tasks::TASKPRIORITY AS TASKPRIORITY,
jedi_tasks::ARCHITECTURE AS ARCHITECTURE,
jedi_tasks::TRANSUSES AS TRANSUSES,
jedi_tasks::TRANSHOME AS TRANSHOME,
jedi_tasks::TRANSPATH AS TRANSPATH,
jedi_tasks::SPLITRULE AS SPLITRULE,
jedi_tasks::WORKQUEUE_ID AS WORKQUEUE_ID,
jedi_tasks::ERRORDIALOG AS ERRORDIALOG,
jedi_tasks::PARENT_TID AS PARENT_TID,
jedi_tasks::EVENTSERVICE AS EVENTSERVICE,
jedi_tasks::TICKETID AS TICKETID,
jedi_tasks::TICKETSYSTEMTYPE AS TICKETSYSTEMTYPE,
jedi_tasks::STATECHANGETIME AS STATECHANGETIME,
jedi_tasks::SUPERSTATUS AS SUPERSTATUS,
jedi_tasks::CAMPAIGN AS CAMPAIGN,
jedi_tasks::GOAL AS GOAL,
jedi_tasks::NUCLEUS AS NUCLEUS,
jedi_tasks::RAMCOUNT as RAMCOUNT,
jedi_tasks::RAMUNIT as RAMUNIT,
jedi_tasks::WALLTIME as WALLTIME,
jedi_tasks::WALLTIMEUNIT as WALLTIMEUNIT,
jedi_tasks::GSHARE as GSHARE,
GetWeek(ToDate(jedi_tasks::STARTTIME)) as WEEK,
((DaysBetween(ToDate(jedi_tasks::STARTTIME),ToDate(0L)) + 4L) % 7) as WEEKDAY;

store prepared into '$out' using parquet.pig.ParquetStorer;
