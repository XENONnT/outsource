#!/usr/bin/env bash


runid=$1
dtype=$2
rse=$3

. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh

export RUCIO_ACCOUNT=production

# untar
tarball=`ls *combined.tar.gz`

echo "tarball: $tarball"

tar xzf $tarball

#echo "contents after untarring:"
#ls -la

echo "contents of 'combined':"
ls -l combined

./rucio_upload.py ${runid} ${dtype} ${rse}

