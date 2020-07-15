#!/usr/bin/env bash

# === arguments - make sure these match Pegasus job definition ===
args=( "$@" )
export run_id=$1
export context=$2
export input_dtype=$3
export output_dtype=$4
export output_tar=$5
export chunks=${args[@]:5}

echo $@



start_dir=$PWD

. /opt/XENONnT/setup.sh


# set GLIDEIN_Country variable if not already
if [[ -z "$GLIDEIN_Country" ]]; then
    export GLIDEIN_Country="US"
fi
#
if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi
#
export RUCIO_ACCOUNT=xenon-analysis
#
#echo "Start dir is $start_dir. Here's whats inside:"
#ls -lah

unset http_proxy
export XENON_CONFIG=$PWD/.xenon_config
# do we still neeed these?
export XDG_CACHE_HOME=${start_dir}/.cache
export XDG_CONFIG_HOME=${start_dir}/.config

echo "--- RUCIO/X509 Stuff ---"
env | grep X509
env | grep RUCIO

echo

echo 'Processing now...'

./runstrax.py ${run_id} --input_dtype ${input_dtype} --output_dtype ${output_dtype} --context ${context} --chunks ${chunk} 2>&1

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 25"
    exit 25
fi

tar czfv ${output_tar} data/*_temp

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo

echo "And here is what is in the data directory:"
ls -lah data/*

