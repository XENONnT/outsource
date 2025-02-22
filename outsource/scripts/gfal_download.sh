#!/bin/bash

set -e

list=$1
workflow=$2

mkdir -p $workflow/outputs

find $workflow/outputs -type f -size 0 -delete

for tarball in $(cat $list); do
    _tarball=$(basename $tarball)
    if [ ! -f $workflow/outputs/$_tarball ]; then
        gfal-copy -f -p -t 7200 -T 7200 $tarball $workflow/outputs/.
    fi
done
