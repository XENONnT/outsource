#!/usr/bin/env bash

set -e

run_id=$1
context=$2
xedocs_version=$3
output_dtype=$4
output_tar=$5
standalone_download=$6
upload_to_rucio=$7
update_db=$8
args=( "$@" )
chunks=${args[@]:8}

echo $@

echo "Processing chunks:"
echo "$chunks"

extraflags=""

if [ "X${standalone_download}" = "Xdownload-only" ]; then
    extraflags="$extraflags --download-only"
elif [ "X${standalone_download}" = "Xno-download" ]; then
    extraflags="$extraflags --no-download"
fi

if [ "X${upload_to_rucio}" = "Xtrue" ]; then
    extraflags="$extraflags --upload-to-rucio"
fi

if [ "X${update_db}" = "Xtrue" ]; then
    extraflags="$extraflags --update-db"
fi

. /opt/XENONnT/setup.sh

# Sleep random amount of time to spread out e.g. API calls and downloads
sleep $(( RANDOM % 20 + 1 ))s


if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

if [ "X$upload_to_rucio" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

echo "Current dir is $PWD. Here's whats inside:"
ls -lah

unset http_proxy
export HOME=$PWD
export XENON_CONFIG=$PWD/.xenon_config
# Do we still neeed these?
export XDG_CACHE_HOME=$PWD/.cache
export XDG_CONFIG_HOME=$PWD/.config

echo "RUCIO/X509 Stuff:"
env | grep X509
env | grep RUCIO

rucio whoami

echo

if [ "X${standalone_download}" = "Xno-download" ]; then
    # We are given a tarball from the previous download job
    echo "Untar input data:"
    tar -xzf *-data*.tar.gz
fi


# Installing customized packages
. install.sh strax straxen cutax


# See if we have any input tarballs
echo "Checking if we have any input tarballs:"
runid_pad=`printf %06d $run_id`
if [ -f ./$runid_pad*.tar.gz ]; then
    mkdir data
    for tarball in $(ls $runid_pad*.tar.gz)
    do
        echo "Untarr input: $tarball:"
        tar -xzf $tarball -C data --strip-components=1
    done
fi
echo

# echo "Check network:"
# echo "ping -c 5 xenon-runsdb.grid.uchicago.edu"
# ping -c 5 xenon-runsdb.grid.uchicago.edu
# echo
echo "Checking if we have .dbtoken:"
echo "ls -lah $HOME/.dbtoken"
ls -lah $HOME/.dbtoken
echo
# echo "nmap xenon-runsdb.grid.uchicago.edu"
# map -p5000 xenon-runsdb.grid.uchicago.edu
# echo

echo "Processing:"

chunkarg=""
if [ -n "${chunks}" ]
then
    chunkarg="--chunks ${chunks}"
fi

time python process.py ${run_id} --context ${context} --xedocs_version ${xedocs_version} --output ${output_dtype} ${extraflags} ${chunkarg}

if [[ $? -ne 0 ]];
then
    echo "Exiting with status 25"
    exit 25
fi

echo "Here is what is in the data directory after processing:"
ls -lah data/*
echo "We want to find and delete any records or records_nv if existing, to save disk in combine jobs."
find data -type d \( -name "*-records-*" -o -name "*-records_nv-*" \) -exec rm -rf {} +

if [ "X${standalone_download}" = "Xdownload-only" ]; then
    echo "We are tarballing the data directory for download-only job:"
    tar czfv ${output_tar} data
elif [ "X${output_dtype}" = "Xevent_info_double" ] || [ "X${output_dtype}" = "Xevents_mv" ] || [ "X${output_dtype}" = "Xevents_nv" ]; then
    echo "We are tarballing the data directory for ${output_dtype} job:"
    tar czfv ${output_tar} data
else
    echo "We are tarballing the data directory, but only for those with _temp:"
    tar czfv ${output_tar} data/*_temp
fi

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo

echo "And here is what is in the data directory:"
ls -lah data/*
