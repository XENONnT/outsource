#!/bin/bash

# This is the first job of the workflow. You can add
# extra checks in here if you want to make sure certain
# things work before starting the processing. The last
# step is to update the db.

export RUNID=$1
export DTYPE=$2
export CONTEXT=$3
export CMT=$4
export update_db=$5
export upload_to_rucio=$6

set -e

combine_extra_args=""

if [ "X$update_db" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --update-db"
fi
if [ "X$upload_to_rucio" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --upload-to-rucio"
fi


# source the environment
. /opt/XENONnT/setup.sh
export XENON_CONFIG=$PWD/.xenon_config
export RUCIO_ACCOUNT=production

# sleep a random amount of time to spread out e.g. API calls
sleep $[ ( $RANDOM % 100 )  + 1 ]s

# ./pre-flight.py $RUNID --dtype $DTYPE --context $CONTEXT --cmt $CMT ${combine_extra_args}
