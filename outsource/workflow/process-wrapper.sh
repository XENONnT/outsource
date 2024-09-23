#!/usr/bin/env bash

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

input_path="input"
mkdir -p $input_path
output_path="output"
mkdir -p $output_path

extraflags=""

if [ "X$standalone_download" = "Xdownload-only" ]; then
    extraflags="$extraflags --download_only"
elif [ "X$standalone_download" = "Xno-download" ]; then
    extraflags="$extraflags --no_download"
fi

if [ "X$rucio_upload" = "Xtrue" ]; then
    extraflags="$extraflags --rucio_upload"
fi

if [ "X$rundb_update" = "Xtrue" ]; then
    extraflags="$extraflags --rundb_update"
fi

chunkarg=""
if [ -n "$chunks" ]
then
    chunkarg="--chunks $chunks"
fi

. /opt/XENONnT/setup.sh

if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

# Sleep random amount of time to spread out e.g. API calls and downloads
sleep $(( RANDOM % 20 + 1 ))s

# Installing customized packages
. install.sh strax straxen cutax utilix outsource

echo "Current dir is $PWD. Here's whats inside:"
ls -lah .

if [ "X$rucio_upload" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

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

run_id_pad=`printf %06d $run_id`

# We are given a tarball from the previous download job
echo "Checking if we have any downloaded input tarballs:"
if [ "X$standalone_download" = "Xno-download" ]; then
    for tarball in $run_id_pad*-download*.tar.gz
    do
        echo "Untarr downloaded input : $tarball:"
        tar -xzf $tarball -C $input_path --strip-components=1
    done
fi
echo

# See if we have any input tarballs
echo "Checking if we have any processed input tarballs:"
for tarball in $run_id_pad*-output*.tar.gz
do
    echo "Untarr input: $tarball:"
    tar -xzf $tarball -C $input_path --strip-components=1
done
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
time python process.py $run_id --context $context --xedocs_version $xedocs_version --data_type $data_type --input_path $input_path --output_path $output_path $extraflags $chunkarg

echo "Here is what is in the output directory after processing:"
ls -lah $output_path
echo "We want to find and delete any records or records_nv if existing, to save disk in combine jobs."
find $output_path -type d \( -name "*-records-*" -o -name "*-records_nv-*" \) -exec rm -rf {} +
echo
echo "Total amount of data before tarballing: "`du -s --si . | cut -f1`
echo

echo "We are tarballing the output directory:"
tar czfv $tar_filename $output_path

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah .
echo
