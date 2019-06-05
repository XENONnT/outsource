#!/bin/bash

# === arguments - make sure these match Pegasus job definition ===
export run_id=$1
export input_dtype=$2
export output_dtype=$3
export output_tar=$4
export chunk=$5


start_dir=$PWD

# set GLIDEIN_Country variable if not already
if [[ -z "$GLIDEIN_Country" ]]; then
    export GLIDEIN_Country="US"
fi

if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

. /opt/XENONnT/setup.sh

echo "Start dir is $start_dir. Here's whats inside:"
ls -lah

unset http_proxy
export HOME=$PWD
# do we still neeed these?
export XDG_CACHE_HOME=${start_dir}/.cache
export XDG_CONFIG_HOME=${start_dir}/.config

echo
echo 'Processing now...'

./runstrax.py ${run_id} ${input_dtype} ${output_dtype} ${chunk} 2>&1

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 25"
    exit 25
fi

tar czf ${output_tar} data/*_temp

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo



