#!/bin/bash

set -e

run_id=$1
context=$2
xedocs_version=$3
rucio_upload=$4
rundb_update=$5
stage=$6
input_path=$7
output_path=$8
staging_dir=$9
tar_filename=${10}
args=( "$@" )
chunks=${args[@]:10}

echo $@
echo $*
echo

echo "Where am i:"
echo $PWD
echo

# Needed by utilix DB
export HOME=$PWD

mkdir -p $input_path
mkdir -p $output_path

extraflags=""

if [ "X$rucio_upload" = "Xtrue" ]; then
    extraflags="$extraflags --rucio_upload"
fi

if [ "X$rundb_update" = "Xtrue" ]; then
    extraflags="$extraflags --rundb_update"
fi

if [ "X$stage" = "Xtrue" ]; then
    extraflags="$extraflags --stage"
fi

chunksarg="--chunks $chunks"

. /opt/XENONnT/setup.sh

if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

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

# The rest of the arguments are the inputs
for tarball in $(ls $run_id_pad*-output*.tar.gz)
do
    tar -xzf $tarball -C $input_path --strip-components=1
    rm $tarball
done

echo "What is in the input directory:"
ls -lah $input_path

echo
echo "Total amount of data before combining: "`du -s --si $input_path | cut -f1`
echo

echo "Combining:"
time python3 combine.py $run_id --context $context --xedocs_version $xedocs_version --input_path $input_path --output_path $output_path --staging_dir $staging_dir $chunksarg $extraflags

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

echo "Here is what is in the output directory after combining:"
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
