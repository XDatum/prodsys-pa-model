#!/usr/bin/env bash

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/create_model_data.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_MODEL_DIR}/bin/run.sh collector --verbose --force --work-dir model_data --config cfg_data --days 90 >> ${LOG_FILE} 2>&1
${P2PA_MODEL_DIR}/bin/run.sh predictor --train --verbose --force --work-dir model_data >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
