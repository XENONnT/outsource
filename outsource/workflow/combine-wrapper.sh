#!/bin/bash

set -e

runid=$1
dtype=$2
context=$3
cmt=$4
update_db=$5
upload_to_rucio=$6

echo $*

combine_extra_args=""

if [ "X$update_db" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --update-db"
fi
if [ "X$upload_to_rucio" = "Xtrue" ]; then
    combine_extra_args="$combine_extra_args --upload-to-rucio"
fi

# the rest of the arguments are the inputs
START=$(date +%s)
for TAR in `ls *.tar.gz`; do
    tar xzf $TAR
done
END=$(date +%s)
DIFF=$(( $END - $START ))

echo "Untarring took $DIFF seconds"

echo "data dir:"
ls -l data

echo
echo
#echo "Total amount of data before combine: "`du -s --si .`
echo
echo

# source the environment
. /opt/XENONnT/setup.sh
export XENON_CONFIG=$PWD/.xenon_config
export RUCIO_ACCOUNT=production


# combine the data
time ./combine.py ${runid} ${dtype} --input data --context ${context} --cmt ${cmt} ${combine_extra_args}

# check data dir again
echo "data dir:"
ls -l data

