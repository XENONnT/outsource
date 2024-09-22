#!/bin/bash

set -e

run_id=$1
context=$2
xedocs_version=$3
output=$4
rucio_upload=$5
rundb_update=$6
args=( "$@" )
chunks=${args[@]:6}

echo $@
echo $*

export HOME=$PWD

combine_extra_args=""

if [ "X$rundb_update" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --rundb_update"
fi
if [ "X$rucio_upload" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --rucio_upload"
fi

# The rest of the arguments are the inputs
START=$(date +%s)
for TAR in `ls *.tar.gz`; do
    tar -xzf $TAR
done
END=$(date +%s)
DIFF=$(( $END - $START ))

echo "Untarring took $DIFF seconds"

echo "data dir:"
ls -l data

echo
echo
echo "Total amount of data before combine: "`du -s --si . | cut -f1`
echo
echo

# source the environment
. /opt/XENONnT/setup.sh

if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

export XENON_CONFIG=$PWD/.xenon_config
if [ "X$rucio_upload" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

# Installing customized packages
. install.sh strax straxen cutax

chunkarg=""
if [ -n "${chunks}" ]
then
    chunkarg="--chunks ${chunks}"
fi

# Combine the data
time python combine.py ${run_id} --context ${context} --xedocs_version ${xedocs_version} --path data ${combine_extra_args} {chunkarg}

# Check data dir again
echo "Here is what is in the data directory after combining:"
ls -l data

# tar up the output
tar czfv ${output} finished_data
