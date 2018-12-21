#!/bin/bash

set -e

TOP_DIR=$1
GENERATED_DIR=$2
RUNS_DIR=$3
RUN_ID=$4

# make sure we are working in the right directory
cd $TOP_DIR

# need basic wn tools for things like transfers
. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/3.3.34/el6-x86_64/setup.sh

# create the site catalog from the template - this has to happen after the local
# env has been fully set up
envsubst < sites.xml.template > $GENERATED_DIR/sites.xml

# make sure we also have access to the AMQP lib
export PYTHONPATH="$PYTHONPATH:/usr/lib/python2.6/site-packages"

# plan and submit the  workflow
pegasus-plan \
    -Dpegasus.catalog.site.file=$GENERATED_DIR/sites.xml \
    --conf pegasus.conf \
    --dir $RUNS_DIR \
    --relative-dir $RUN_ID \
    --sites condorpool \
    --staging-site staging \
    --output-site local \
    --dax $GENERATED_DIR/dax.xml \
    --cluster horizontal \
    --submit

