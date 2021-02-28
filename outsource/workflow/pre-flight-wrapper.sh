#!/bin/bash

# This is the first job of the workflow. You can add
# extra checks in here if you want to make sure certain
# things work before starting the processing. The last
# step is to update the db.

export RUNID=$1
export DTYPE=$2
export CONTEXT=$3
export RSE=$4

set -e

# source the environment
. /opt/XENONnT/setup.sh
export XENON_CONFIG=$PWD/.xenon_config
export RUCIO_ACCOUNT=production

./pre-flight.py $RUNID --dtype $DTYPE --context $CONTEXT --rse $RSE


# rundb...
#python $BASE_DIR/workflow/update_run_db.py update-status --run-id=$RUN_NUMBER --dtype=records --status=processing


 
