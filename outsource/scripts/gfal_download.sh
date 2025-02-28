#!/bin/bash

set -e

list=$1
workflow=$2
relay=$3

mkdir -p $workflow/outputs

find $workflow/outputs -type f -size 0 -delete

if [ "X$relay" = "Xrelay" ]; then
    # IF OSG-RCC relay, do not download per-chunk files
    tarballs=($(cat $list | grep output.tar.gz))
else
    # If not OSG-RCC relay, download all files
    tarballs=($(cat $list))
fi

for tarball in ${tarballs[@]}; do
    _tarball=$(basename $tarball)
    if [ ! -f $workflow/outputs/$_tarball ]; then
        gfal-copy -f -p -t 7200 -T 7200 $tarball $workflow/outputs/.
    fi
done
