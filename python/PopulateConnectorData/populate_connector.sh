#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
SCRIPT_HOME=${SCRIPT_DIR}/../../
TENANT_HOST=debug.gaiaserve.net:8443
TOKEN=integration_tests_token_admin_role_8486982342642489461200933204202551682525

export PYTHONPATH="${PYTHONPATH}:${SCRIPT_HOME}"
pushd ${SCRIPT_HOME}/python/PopulateConnectorData
/usr/bin/python2.7 populate.py --host ${TENANT_HOST} --token ${TOKEN}
popd
