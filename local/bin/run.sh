#!/usr/bin/env bash

handlername=$1

SERVICE_EXEC_USER=matitov
ANALYTICS_CLUSTER=analytix.cern.ch

P2PA_CRONJOBS_DIR=${P2PA_CRONJOBS_DIR:-/home/cronjobs}
SERVICE_KEYTAB=${P2PA_CRONJOBS_DIR}/.globus/${SERVICE_EXEC_USER}.keytab
#SERVICE_BASE_DIR=${P2PA_CRONJOBS_DIR}/services/prodsys-pa-model

P2PA_EXEC_DIR=/afs/cern.ch/user/${SERVICE_EXEC_USER:0:1}/${SERVICE_EXEC_USER}/prodsys-pa-model
P2PA_EXEC_MODULE="$P2PA_EXEC_DIR/bin/$handlername.py ${@:2}"
P2PA_CREDS_ENV=${P2PA_EXEC_DIR}/private/creds_env.sh

kinit -k -t ${SERVICE_KEYTAB} ${SERVICE_EXEC_USER}@CERN.CH
aklog
ssh -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no \
    -o LogLevel=error \
    -o GSSAPIAuthentication=yes \
    -o GSSAPIDelegateCredentials=yes \
    ${SERVICE_EXEC_USER}@${ANALYTICS_CLUSTER} \
    "source $P2PA_CREDS_ENV; export PYTHONPATH=$PYTHONPATH:$P2PA_EXEC_DIR; $P2PA_EXEC_MODULE"
kdestroy
