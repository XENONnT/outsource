#!/usr/bin/env bash

# === arguments - make sure these match Pegasus job definition ===
args=( "$@" )
export run_id=$1
export context=$2
export output_dtype=$3
export output_tar=$4
export standalone_download=$5
export upload_to_rucio=$6
export update_db=$7
export chunks=${args[@]:7}

echo $@

echo "Chunks: $chunks"
start_dir=$PWD

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

# sleep random amount of time to spread out e.g. API calls and downloads
sleep $[ ( $RANDOM % 20 )  + 1 ]s


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
export RUCIO_ACCOUNT=production
#
echo "Start dir is $start_dir. Here's whats inside:"
ls -lah

unset http_proxy
export XENON_CONFIG=$PWD/.xenon_config
# do we still neeed these?
export XDG_CACHE_HOME=${start_dir}/.cache
export XDG_CONFIG_HOME=${start_dir}/.config

echo "--- RUCIO/X509 Stuff ---"
env | grep X509
env | grep RUCIO

rucio whoami

echo

if [ "X${standalone_download}" = "Xno-download" ]; then
    # we are given a tarball from the previous download job
    echo 'Untaring input data...'
    tar xzf *-data-*.tar.gz
fi


echo "--- Installing cutax ---"
mkdir cutax
tar -xzf cutax.tar.gz -C cutax --strip-components=1
pip install ./cutax --user --no-deps -qq
python -c "import cutax; print(cutax.__file__)"


# see if we have any input tarballs
echo "--- Checking if we have any input tarballs ---"
runid_pad=`printf %06d $run_id`
if [ -f ./$runid_pad*.tar.gz ]; then
  mkdir data
  for tarball in $(ls $runid_pad*.tar.gz)
  do
    echo "Untarring input: $tarball"
    tar xzf $tarball -C data --strip-components=1
  done
fi
echo

echo 'Processing now...'

chunkarg=""
if [ -n "${chunks}" ]
then
  chunkarg="--chunks ${chunks}"
fi

./runstrax.py ${run_id} --output ${output_dtype} --context ${context} ${extraflags} ${chunkarg}

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 25"
    exit 25
fi

echo "Here is what is in the data directory after processing:"
ls -lah data/*


if [ -z "${chunks}" ]
then
  echo "No chunks passed, so exiting now with status 0"
  exit 0
fi


if [ "X${standalone_download}" = "Xdownload-only" ]; then
    tar czfv ${output_tar} data
else
    tar czfv ${output_tar} data/*_temp
fi

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo

echo "And here is what is in the data directory:"
ls -lah data/*

