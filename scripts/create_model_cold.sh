#!/usr/bin/env bash

MODEL_WORK_DIR=model_cold
QUERY_CONFIG=cfg_model_cold

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/create_model_cold.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_MODEL_DIR}/bin/run.sh collector --verbose --force --work-dir ${MODEL_WORK_DIR} --config ${QUERY_CONFIG} --days 90 >> ${LOG_FILE} 2>&1
${P2PA_MODEL_DIR}/bin/run.sh predictor --train --verbose --force --work-dir ${MODEL_WORK_DIR} >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
