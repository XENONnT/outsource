#!/bin/bash

# This is the first job of the workflow. You can add
# extra checks in here if you want to make sure certain
# things work before starting the processing. The last
# step is to update the db.

BASE_DIR=$1
RUN_NUMBER=$2

. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh

# checks...


# rundb...
#python $BASE_DIR/workflow/update_run_db.py update-status --run-id=$RUN_NUMBER --dtype=records --status=processing


 
