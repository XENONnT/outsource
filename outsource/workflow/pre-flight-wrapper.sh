#!/bin/bash

# This is the first job of the workflow. You can add
# extra checks in here if you want to make sure certain
# things work before starting the processing. The last
# step is to update the db.

export RUNID=$1
export DTYPE=$2
export CONTEXT=$3
export RSE=$4
export CMT=$5
export UPDATE_DB=$6

set -e

# source the environment
. /opt/XENONnT/setup.sh
export XENON_CONFIG=$PWD/.xenon_config
export RUCIO_ACCOUNT=production

# sleep a random amount of time to spread out e.g. API calls
sleep $[ ( $RANDOM % 20 )  + 1 ]s

if [ "X$UPDATE_DB" = "Xtrue" ]; then
    ./pre-flight.py $RUNID --dtype $DTYPE --context $CONTEXT --rse $RSE --cmt $CMT
fi

