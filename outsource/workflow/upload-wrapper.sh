#!/usr/bin/env bash

set -e

runid=$1
dtype=$2
rse=$3
context=$4

# source the environment
. /opt/XENONnT/setup.sh

# . /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh

export RUCIO_ACCOUNT=production
export XENON_CONFIG=$PWD/.xenon_config

rucio whoami

env | grep X509

# untar
tarball=`ls *combined.tar.gz`

echo "tarball: $tarball"

tar xzf $tarball

echo "contents of 'combined':"
ls -l combined/*


if [[ !  $? -eq 0 ]]
then
    echo "combined does not exist. Exiting."
    exit 2
fi

# rynge can not upload - but want to test everything except this step
if [ "X$PEGASUS_SUBMITTING_USER" != "Xrynge" ]; then
    ./upload.py ${runid} ${dtype} ${rse} --context ${context}
fi

