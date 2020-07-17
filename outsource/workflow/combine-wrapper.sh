#!/bin/bash

set -e

runid=$1
dtype=$2
context=$4
rse=$5

echo $*

shift
# the rest of the arguments are the inputs

for TAR in `ls *.tar.gz`; do
    tar xzf $TAR
done

ls -la

echo "---- data dir ----"
ls -l data

echo
echo
echo "Total amount of data before combine: "`du -s --si .`
echo
echo

# source the environment
. /opt/XENONnT/setup.sh

export XENON_CONFIG=$PWD/.xenon_config

# combine the data
./combine.py ${runid} ${dtype} --input_path data --output_path combined --context ${context}
# upload to rucio and update runDB
./upload.py ${runid} ${dtype} ${rse} --context ${context}


