#!/bin/bash

set -e

workflow=$1
relay=$2

# Define directories
outputs=$workflow/outputs
del=$workflow/del
lower_log=lower_done.log
upper_log=upper_done.log
done_log=done.log

# Ensure the delete folder exists
mkdir -p $del

# Get the run IDs from the files
ls $outputs | grep output.tar.gz | grep -v log | grep let | cut -d'/' -f11 | cut -d'-' -f1 | sort | uniq > $lower_log
ls $outputs | grep output.tar.gz | grep -v log | grep event | cut -d'/' -f11 | cut -d'-' -f1 | sort | uniq > $upper_log
comm -12 $lower_log $upper_log > $done_log

if [ ! -s "$lower_log" ]; then
    echo "Upper-level only"
    exit 0
fi

# Read and store unique run IDs into an array
if [ "X$relay" = "Xrelay" ]; then
    # IF OSG-RCC relay, remove runs whose upper finished only
    run_ids=($(cat $upper_log | sort | uniq))
else
    # If not OSG-RCC relay, keep runs whose both lower and upper finished
    run_ids=($(cat $lower_log $upper_log | sort | uniq))
fi

# Read each run ID from the file
for run_id in ${run_ids[@]}; do
    # Find files matching the run ID
    matching_files=($(ls $outputs | grep output.tar.gz | grep -v log | grep $run_id))

    # Check the number of matching files
    if [ ${#matching_files[@]} -ne 2 ]; then
        for file in "${matching_files[@]}"; do
            mv $outputs/$file $del/
            echo "Moved: $outputs/$file -> $del/"
        done
    fi
done
