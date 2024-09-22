#!/usr/bin bash

set -e

run_id=$1
context=$2
xedocs_version=$3
data_type=$4
standalone_download=$5
rucio_upload=$6
rundb_update=$7
tar_filename=$8
args=( "$@" )
chunks=${args[@]:8}

echo $@
echo $*

export HOME=$PWD

echo "Processing chunks:"
echo "$chunks"

extraflags=""

if [ "X${standalone_download}" = "Xdownload-only" ]; then
    extraflags="$extraflags --download_only"
elif [ "X${standalone_download}" = "Xno-download" ]; then
    extraflags="$extraflags --no_download"
fi

if [ "X${rucio_upload}" = "Xtrue" ]; then
    extraflags="$extraflags --rucio_upload"
fi

if [ "X${rundb_update}" = "Xtrue" ]; then
    extraflags="$extraflags --rundb_update"
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

if [ "X$rucio_upload" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

# Installing customized packages
. install.sh strax straxen cutax outsource

echo "Current dir is $PWD. Here's whats inside:"
ls -lah

unset http_proxy
export XENON_CONFIG=$PWD/.xenon_config
# Do we still neeed these?
export XDG_CACHE_HOME=$PWD/.cache
export XDG_CONFIG_HOME=$PWD/.config

echo "RUCIO/X509 Stuff:"
env | grep X509
env | grep RUCIO

rucio whoami

echo

mkdir -p data

# We are given a tarball from the previous download job
echo "Checking if we have any downloaded input tarballs:"
if [ "X${standalone_download}" = "Xno-download" ]; then
    for tarball in $(ls *-data-*.tar.gz)
    do
        echo "Untarr downloaded input : $tarball:"
        tar -xzf $tarball -C data --strip-components=1
    done
fi
echo

# See if we have any input tarballs
echo "Checking if we have any input tarballs:"
run_id_pad=`printf %06d $run_id`
if [ -f ./$run_id_pad*.tar.gz ]; then
    for tarball in $(ls $run_id_pad*.tar.gz)
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
echo "ls -lah $PWD/.dbtoken"
ls -lah $PWD/.dbtoken
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

time python process.py ${run_id} --context ${context} --xedocs_version ${xedocs_version} --data_type ${data_type} --output_path data ${extraflags} ${chunkarg}

if [[ $? -ne 0 ]];
then
    echo "Exiting with status 25"
    exit 25
fi

echo "Here is what is in the data directory after processing:"
ls -lah data/*

if [ "X${standalone_download}" = "Xdownload-only" ]; then
    echo "We are tarballing the data directory for download_only job:"
    tar czfv ${tar_filename} data
else
    echo "We are tarballing the data directory for ${data_type} job:"
    tar czfv ${tar_filename} data
fi

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo

echo "And here is what is in the data directory:"
ls -lah data/*
