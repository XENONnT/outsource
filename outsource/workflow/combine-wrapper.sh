#!/bin/bash

set -e

runid=$1
dtype=$2
context=$3
rse=$4
dbflag=
rucioflag=

echo $*

# check if we passed any flags to ignore rundb and/or ignore upload
options=$(getopt -l "ignore-db,ignore-rucio" -a -o "dr" -- $@)
eval set -- "$options"

update_db=true
upload_rucio=true

while true; do
    case $1 in
        --ignore-db) export update_db=false; export dbflag='--ignore-db' ;;
        --ignore-rucio) export upload_rucio=false; export rucioflag='--ignore-rucio';;
        --) break ;;
    esac
    shift
done


shift
# the rest of the arguments are the inputs
START=$(date +%s)
for TAR in `ls *.tar.gz`; do
    tar xzf $TAR
done
END=$(date +%s)
DIFF=$(( $END - $START ))

echo "Untarring took $DIFF seconds"

echo "data dir:"
ls -l data

echo
echo
#echo "Total amount of data before combine: "`du -s --si .`
echo
echo

# source the environment
. /opt/XENONnT/setup.sh
export XENON_CONFIG=$PWD/.xenon_config
export RUCIO_ACCOUNT=production


# combine the data
time ./combine.py ${runid} ${dtype} --input data --context ${context} --rse ${rse} ${dbflag} ${rucioflag}

# check data dir again
echo "data dir:"
ls -l data

# upload to rucio and update runDB unless specified otherwise
#if [ $upload_rucio == 'true' ]; then
#  time ./upload.py ${runid} ${dtype} ${rse} --context ${context} ${dbflag}
#else
#  echo "Skipping rucio upload due to --ignore-rucio flag"
#fi


