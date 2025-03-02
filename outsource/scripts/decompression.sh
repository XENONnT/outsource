#!/bin/bash

set -e

workflow=$1

mkdir -p $workflow/outputs/logs
mv $workflow/outputs/*.log $workflow/outputs/logs/.

mkdir -p $workflow/outputs/strax_data_osg
mkdir -p $workflow/outputs/strax_data_osg_per_chunk

for tarball in $(ls $workflow/outputs | grep output); do
    # run_id=$( echo $tarball | cut -d '-' -f 1 )
    # run_id=$( echo $run_id | sed 's/^0*//' )
    # if [[ $run_id -lt 54000 ]]; then
    #     continue
    # fi
    # echo $tarball
    if [[ $tarball == *-output.tar.gz ]]; then
        destination=$workflow/outputs/strax_data_osg
    elif [[ $tarball == *-output-*.tar.gz ]]; then
        destination=$workflow/outputs/strax_data_osg_per_chunk
    else
        echo "Unknown tarball format: $tarball"
        exit 1
    fi
    tar -xvzf $workflow/outputs/$tarball -C $destination --strip-components=1
    # Move resources testing files to the outputs folder
    mv $destination/*.npy $workflow/outputs/.
    mv $destination/*.json $workflow/outputs/.
done
