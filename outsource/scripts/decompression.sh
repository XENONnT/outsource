#!/bin/bash

set -e

workflow=$1

mkdir -p $workflow/outputs/logs

if ls $workflow/outputs/*.log >/dev/null 2>&1; then
    mv $workflow/outputs/*.log $workflow/outputs/logs/.
fi

mkdir -p $workflow/outputs/strax_data_osg
mkdir -p $workflow/outputs/strax_data_osg_per_chunk
mkdir -p $workflow/outputs/temp

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
    # tar -xvzf $workflow/outputs/$tarball -C $destination --strip-components=1
    if ! tar -xzf $workflow/outputs/$tarball -C $workflow/outputs/temp --strip-components=1; then
        echo "Failed to extract: $workflow/outputs/$tarball"
        exit 1
    else
        mv $workflow/outputs/temp/* $destination/.
        echo "Extracted: $workflow/outputs/$tarball -> $destination"
        rm $workflow/outputs/$tarball
        echo "Removed: $workflow/outputs/$tarball"
    fi
    # Move resources testing files to the outputs folder
    mv $destination/*.npy $workflow/outputs/.
    mv $destination/*.json $workflow/outputs/.
done
