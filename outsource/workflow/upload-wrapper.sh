#!/usr/bin/env bash


runid=$1
dtype=$2
rse=$3

. /cvmfs/xenon.opensciencegrid.org/testing/releases/latest/setup.sh

export RUCIO_ACCOUNT=production

# untar
tarball=`ls *merged.tar.gz`

echo "tarball: $tarball"

tar xzf $tarball

#echo "contents after untarring:"
#ls -la

echo "contents of 'merged':"
ls -l merged

./rucio_upload.py ${runid} ${dtype} ${rse}

