#!/usr/bin/env bash

set -e

run_id=$1
context=$2
xedocs_version=$3
rucio_upload=$4
rundb_update=$5
tar_filename=$6
args=( "$@" )
chunks=${args[@]:6}

echo $@
echo $*

export HOME=$PWD

input_path="input"
mkdir -p $input_path
output_path="output"
mkdir -p $output_path

extraflags=""

if [ "X$rundb_update" = "Xtrue" ]; then
    extraflags="$extraflags --rundb_update"
fi
if [ "X$rucio_upload" = "Xtrue" ]; then
    extraflags="$extraflags --rucio_upload"
fi

chunksarg="--chunks $chunks"

. /opt/XENONnT/setup.sh

if [ -e /image-build-info.txt ]; then
    echo
    echo "Running in image with build info:"
    cat /image-build-info.txt
    echo
fi

# Installing customized packages
. install.sh strax straxen cutax utilix outsource

echo "Current dir is $PWD. Here's whats inside:"
ls -lah .

if [ "X$rucio_upload" = "Xtrue" ]; then
    export RUCIO_ACCOUNT=production
fi

unset http_proxy
export XENON_CONFIG=$PWD/.xenon_config

echo "RUCIO/X509 Stuff:"
env | grep X509
env | grep RUCIO

rucio whoami

echo

run_id_pad=`printf %06d $run_id`

# The rest of the arguments are the inputs
for TAR in $(ls $run_id_pad*-output*.tar.gz)
do
    tar -xzf $TAR -C $input_path --strip-components=1
    rm $TAR
done

echo "What is in the input directory:"
ls -lah $input_path

echo
echo "Total amount of data before combine: "`du -s --si $input_path | cut -f1`
echo

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export GOTO_NUM_THREADS=1

echo "Combining:"
time python3 combine.py $run_id --context $context --xedocs_version $xedocs_version --input_path $input_path --output_path $output_path $chunksarg $extraflags

echo "Removing inputs directory:"
rm -r $input_path

echo "Here is what is in the output directory after combining:"
ls -lah $output_path

echo
echo "Total amount of data before tarballing: "`du -s --si $output_path | cut -f1`
echo

echo "We are tarballing the output directory:"
tar czfv $tar_filename $output_path

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah .
echo
