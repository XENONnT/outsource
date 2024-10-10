#!/usr/bin/env bash

set -e

run_id=$1
context=$2
xedocs_version=$3
chunks_start=$4
chunks_end=$5
rucio_upload=$6
rundb_update=$7
ignore_processed=$8
standalone_download=$9
tar_filename=${10}
args=( "$@" )
data_types=${args[@]:10}

echo $@
echo $*

export HOME=$PWD

echo "Processing chunks:"
echo "$chunks_start to $chunks_end"

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

if [ "X$ignore_processed" = "Xtrue" ]; then
    extraflags="$extraflags --ignore_processed"
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

echo "RUCIO/X509 Stuff:"
env | grep X509
env | grep RUCIO

rucio whoami

echo

run_id_pad=`printf %06d $run_id`

# We are given a tarball from the previous download job
echo "Checking if we have any downloaded input tarballs:"
if [ "X$standalone_download" = "Xno-download" ]; then
    for tarball in $(ls $run_id_pad*-download*.tar.gz)
    do
        echo "Untarr downloaded input : $tarball:"
        tar -xzf $tarball -C $input_path --strip-components=1
        rm $tarball
    done
fi
echo

# See if we have any input tarballs
echo "Checking if we have any processed input tarballs:"
for tarball in $(ls $run_id_pad*-output*.tar.gz)
do
    echo "Untarr input: $tarball:"
    tar -xzf $tarball -C $input_path --strip-components=1
    rm $tarball
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

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export GOTO_NUM_THREADS=1
# Limiting CPU usage of TensorFlow
export TF_NUM_INTRAOP_THREADS=1
export TF_NUM_INTEROP_THREADS=1
# For unknown reason, this does not work on limiting CPU usage of JAX
export XLA_FLAGS="--xla_cpu_multi_thread_eigen=false intra_op_parallelism_threads=1"
# But this works, from https://github.com/jax-ml/jax/discussions/22739
export NPROC=1

echo "Processing:"
time python3 process.py $run_id --context $context --xedocs_version $xedocs_version --chunks_start $chunks_start --chunks_end $chunks_end --input_path $input_path --output_path $output_path --data_types $data_types $extraflags

echo "Removing inputs directory:"
rm -r $input_path

echo "Here is what is in the output directory after processing:"
ls -lah $output_path

echo
echo "Total amount of data before tarballing: "`du -s --si $output_path | cut -f1`
echo

echo "We are tarballing the output directory:"
tar czfv $tar_filename $output_path

echo "Removing outputs directory:"
rm -r $output_path

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah .
echo
