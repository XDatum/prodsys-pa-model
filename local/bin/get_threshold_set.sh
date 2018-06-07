#!/usr/bin/env bash

WORK_DIR=threshold_set
QUERY_CONFIG=cfg_threshold_set
DISTR_METHOD=set_ttcr_dict

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/get_threshold_set.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh collector --verbose --force --work-dir ${WORK_DIR} --config ${QUERY_CONFIG} --days 180 --output-type output >> ${LOG_FILE} 2>&1
${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh distributor --verbose --data-dir ${WORK_DIR} --method ${DISTR_METHOD} >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
