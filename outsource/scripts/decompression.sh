#!/bin/bash

set -e

workflow=$1

mkdir -p $workflow/outputs/strax_data

for tarball in $(ls $workflow | grep tar); do
    # run_id=$( echo $tarball | cut -d '-' -f 1 )
    # run_id=$( echo $run_id | sed 's/^0*//' )
    # if [[ $run_id -lt 54000 ]]; then
    #     continue
    # fi
    # echo $tarball
    tar -xvzf $workflow/$tarball -C $workflow/outputs/strax_data --strip-components=1
    # Move resources testing files to the outputs folder
    mv $workflow/outputs/strax_data/*.npy $workflow/outputs/.
    mv $workflow/outputs/strax_data/*.json $workflow/outputs/.
done
