#!/usr/bin/env bash

MODEL_WORK_DIR=model_cold
NEW_TASKS_WORK_DIR=new_tasks_cold
COLLECTOR_QUERY_CONFIG=cfg_new_tasks_cold
PREDICTOR_QUERY_CONFIG=cfg_cold
DISTR_METHOD=set_ttcj_dict

LOG_FILE=${P2PA_CRONJOBS_LOG_DIR}/generate_predictions_cold.log

echo "$(date +"%Y-%m-%d %H:%M:%S") - starting" > ${LOG_FILE} 2>&1

${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh collector --verbose --force --work-dir ${NEW_TASKS_WORK_DIR} --config ${COLLECTOR_QUERY_CONFIG} --days 1 --output-type input >> ${LOG_FILE} 2>&1
${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh predictor --verbose --force --work-dir ${MODEL_WORK_DIR} --data-dir ${NEW_TASKS_WORK_DIR} --config ${PREDICTOR_QUERY_CONFIG} >> ${LOG_FILE} 2>&1
${P2PA_CRONJOBS_DIR}/bin/p2pamodel_run.sh distributor --verbose --data-dir ${NEW_TASKS_WORK_DIR} --method ${DISTR_METHOD} >> ${LOG_FILE} 2>&1

echo "$(date +"%Y-%m-%d %H:%M:%S") - finished" >> ${LOG_FILE} 2>&1
