#!/bin/bash

# This is the first job of the workflow. You can add
# extra checks in here if you want to make sure certain
# things work before starting the processing. The last
# step is to update the db.

BASE_DIR=$1
RUN_NUMBER=$2

. /cvmfs/xenon.opensciencegrid.org/testing/releases/latest/setup.sh

# checks...


# rundb...
cd $BASE_DIR/outsource/workflow/
python update_run_db.py $RUN_NUMBER

 
