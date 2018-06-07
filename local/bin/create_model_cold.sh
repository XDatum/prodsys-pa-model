#!/usr/bin/env bash

MODEL_WORK_DIR=model_cold
COLLECTOR_QUERY_CONFIG=cfg_model_cold
PREDICTOR_QUERY_CONFIG=cfg_cold

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/create_model_cold.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh collector --verbose --force --work-dir ${MODEL_WORK_DIR} --config ${COLLECTOR_QUERY_CONFIG} --days 180 >> ${LOG_FILE} 2>&1
${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh predictor --train --verbose --force --work-dir ${MODEL_WORK_DIR} --config ${PREDICTOR_QUERY_CONFIG} >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
