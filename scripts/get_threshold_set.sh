#!/usr/bin/env bash

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/get_threshold_set.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_MODEL_DIR}/bin/run.sh collector --verbose --force --work-dir threshold_set --config cfg_threshold_set --days 180 --output-type output >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
