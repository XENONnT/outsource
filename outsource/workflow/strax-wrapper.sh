#!/usr/bin/env bash

# === arguments - make sure these match Pegasus job definition ===
args=( "$@" )
export run_id=$1
export context=$2
export cmt=$3
export output_dtype=$4
export output_tar=$5
export rse=$6
export dbflag=
export rucioflag=
export chunks=${args[@]:6}

echo $@

echo "Chunks: $chunks"
start_dir=$PWD

# check if we passed any flags to ignore rundb and/or ignore upload
options=$(getopt -l "ignore-db,ignore-rucio" -a -o "dr" -- $@)
eval set -- "$options"

update_db=true
upload_rucio=true

while true; do
    case $1 in
        --ignore-db) export dbflag='--ignore-db' ;;
        --ignore-rucio) export rucioflag='--ignore-rucio';;
        --) break ;;
    esac
    shift
done


. /opt/XENONnT/setup.sh

# sleep random amount of time to spread out e.g. API calls and downloads
sleep $[ ( $RANDOM % 30 )  + 1 ]s


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

echo 'Processing now...'

chunkarg=""
if [ -n "${chunks}" ]
then
  chunkarg="--chunks ${chunks}"
fi

./runstrax.py ${run_id} --output ${output_dtype} --context ${context} --cmt ${cmt} --rse ${rse} ${chunkarg}

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


tar czfv ${output_tar} data/*_temp

echo
echo "Job is done. Here is the contents of the directory now:"
ls -lah
echo

echo "And here is what is in the data directory:"
ls -lah data/*

