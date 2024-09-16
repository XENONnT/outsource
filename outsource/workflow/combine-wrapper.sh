#!/bin/bash

set -e

run_id=$1
dtype=$2
context=$3
xedocs_version=$4
output=$5
update_db=$5
upload_to_rucio=$7

export HOME=$PWD

echo $*

combine_extra_args=""

if [ "X$update_db" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --update-db"
fi
if [ "X$upload_to_rucio" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --upload-to-rucio"
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
export XENON_CONFIG=$PWD/.xenon_config
if [ "X$upload_to_rucio" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

# Installing customized packages
. install.sh

# Combine the data
time python combine.py ${run_id} ${dtype} --context ${context} --xedocs_version ${xedocs_version} --input data ${combine_extra_args}

# Check data dir again
echo "data dir:"
ls -l data

# tar up the output
tar -czfv ${output} finished_data
