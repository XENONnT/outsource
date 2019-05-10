#!/bin/bash

set -e

runid=$1
dtype=$2
output_file=$3

echo $*

shift
# the rest of the arguments are the inputs

for TAR in `ls *.tar.gz`; do
    tar xzf $TAR
done

ls -la

echo "---- data dir ----"
ls -l data

# source the environment
. /opt/XENONnT/setup.sh

./merge.py ${runid} ${dtype} --input_path data --output_path merged

# create the new output file
tar czf ${output_file} merged

