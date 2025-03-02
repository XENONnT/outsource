#!/bin/bash

set -e

run_id=$1
context=$2
xedocs_version=$3
chunks_start=$4
chunks_end=$5
rucio_upload=$6
rundb_update=$7
ignore_processed=$8
stage=$9
download_heavy=${10}
remove_heavy=${11}
input_path=${12}
output_path=${13}
staging_dir=${14}
tar_filename=${15}
args=( "$@" )
data_types=${args[@]:15}

echo $@
echo $*
echo

echo "Where am i:"
echo $PWD
echo

# Needed by utilix DB
export HOME=$PWD

echo "Processing chunks:"
echo "$chunks_start to $chunks_end"

mkdir -p $input_path
mkdir -p $output_path

extraflags=""

if [ "X$rucio_upload" = "Xtrue" ]; then
    extraflags="$extraflags --rucio_upload"
fi

if [ "X$rundb_update" = "Xtrue" ]; then
    extraflags="$extraflags --rundb_update"
fi

if [ "X$ignore_processed" = "Xtrue" ]; then
    extraflags="$extraflags --ignore_processed"
fi

if [ "X$stage" = "Xtrue" ]; then
    extraflags="$extraflags --stage"
fi

if [ "X$download_heavy" = "Xtrue" ]; then
    extraflags="$extraflags --download_heavy"
fi

if [ "X$remove_heavy" = "Xtrue" ]; then
    extraflags="$extraflags --remove_heavy"
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

unset http_proxy
export XENON_CONFIG=$PWD/.xenon_config

if [ -f install.sh ]; then
    # Installing customized packages
    . install.sh strax straxen cutax utilix admix outsource
fi

echo "Current dir is $PWD. Here's whats inside:"
ls -lah .
echo

if [ "X$rucio_upload" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

echo "PEGASUS Stuff:"
env | grep PEGASUS
echo

echo "XENON Stuff:"
env | grep XENON
echo

echo "RUCIO/X509 Stuff:"
env | grep RUCIO
env | grep X509
echo

rucio whoami
echo

run_id_pad=`printf %06d $run_id`

# We are given a tarball from the previous job
echo "Checking if we have any downloaded input tarballs:"
for tarball in $(ls $run_id_pad*-download*.tar.gz)
do
    echo "Untar downloaded input : $tarball:"
    tar -xzf $tarball -C $input_path --strip-components=1
    rm $tarball
done
echo

# See if we have any input tarballs
echo "Checking if we have any processed input tarballs:"
for tarball in $(ls $run_id_pad*-output*.tar.gz)
do
    echo "Untar input: $tarball:"
    tar -xzf $tarball -C $input_path --strip-components=1
    rm $tarball
done
echo

echo "What is in the input directory:"
ls -lah $input_path

echo
echo "Total amount of data before processing: "`du -s --si $input_path | cut -f1`
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
time python3 process.py $run_id --context $context --xedocs_version $xedocs_version --chunks_start $chunks_start --chunks_end $chunks_end --input_path $input_path --output_path $output_path --staging_dir $staging_dir --data_types $data_types $extraflags
# time admix-download $run_id raw_records --chunks $(seq -s ' ' $chunks_start $(($chunks_end - 1))) --tries 3 --threads 1 --hash rfzvpzj4mf --stage

echo
echo "Moving auxiliary files to output directory"
if ls $input_path/*.npy >/dev/null 2>&1; then mv $input_path/*.npy $output_path; fi
if ls $input_path/*.json >/dev/null 2>&1; then mv $input_path/*.json $output_path; fi
if ls *.npy >/dev/null 2>&1; then mv *.npy $output_path; fi
if ls *.json >/dev/null 2>&1; then mv *.json $output_path; fi

echo
echo "Total amount of data in $input_path before removing: "`du -s --si $input_path | cut -f1`
echo

# There will not be storage pressure if no tarball is produced
if [ $tar_filename != "X" ]; then
    echo "Removing inputs directory:"
    rm -r $input_path
fi

echo "Here is what is in the output directory after processing:"
ls -lah $output_path

echo
echo "Total amount of data in $output_path before tarballing: "`du -s --si $output_path | cut -f1`
echo

if [ $tar_filename != "X" ]; then
    echo "We are tarballing the output directory and removing it:"
    tar czfv $tar_filename $output_path
    # tar czfv $tar_filename $output_path --remove-files
fi

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah .
echo
