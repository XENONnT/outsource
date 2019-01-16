#!/bin/bash

set -e

export PEGASUS_HOME=$1
export TOP_DIR=$2
export WORK_DIR=$3
export GENERATED_DIR=$4
export RUNS_DIR=$5
export RUN_ID=$6

# make sure we are working in the right directory
cd $TOP_DIR

# make sure we have a pretty clean environment
export PATH=$PEGASUS_HOME/bin:/usr/bin:/bin
unset LD_LIBRARY_PATH
unset PYTHONPATH

# need basic wn tools for things like transfers
. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/3.4.22/el6-x86_64/setup.sh

# create the site catalog from the template - this has to happen after the local
# env has been fully set up
envsubst < workflow/sites.xml.template > $GENERATED_DIR/sites.xml

# make sure we also have access to the AMQP lib
export PYTHONPATH="$PYTHONPATH:/usr/lib/python2.6/site-packages"

# plan and submit the  workflow
pegasus-plan \
    -Dpegasus.catalog.site.file=$GENERATED_DIR/sites.xml \
    --conf workflow/pegasus.conf \
    --dir $RUNS_DIR \
    --relative-dir $RUN_ID \
    --sites condorpool \
    --staging-site staging \
    --output-site local \
    --dax $GENERATED_DIR/dax.xml \
    --cluster horizontal \
    --submit

