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


#### If data is in Rucio, find the rse to use
###if [[ $rucio_dataset != "None" ]]; then
###
###    echo "python ${start_dir}/determine_rse.py ${rucio_dataset} $GLIDEIN_Country" 
###    rse=$(python ${start_dir}/determine_rse.py ${rucio_dataset} $GLIDEIN_Country)
###    if [[ $? != 0 ]]; then
###        # disable rucio downloading
###        echo "WARNING: determine_rse.py call failed - disabling Rucio downloading"
###        rucio_dataset="None"
###    fi
###fi

echo "Start dir is $start_dir. Here's whats inside:"
ls -la

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
ls -l
echo



