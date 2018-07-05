#!/usr/bin/env bash

WORK_DIR=eval_cold
QUERY_CONFIG=cfg_eval_cold

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/get_eval_cold.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh collector --verbose --force --work-dir ${WORK_DIR} --config ${QUERY_CONFIG} --days 14 --days-offset 14 --output-type output >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
