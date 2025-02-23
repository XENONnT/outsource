#!/bin/bash

set -e

workflow=$1

# Define directories
outputs=$workflow/outputs
del=$workflow/del
lower_log=lower_done.log
upper_log=upper_done.log
done_log=done.log

# Get the run IDs from the files
ls $outputs | grep let | cut -d'/' -f11 | cut -d'-' -f1 | sort | uniq > $lower_log
ls $outputs | grep event | cut -d'/' -f11 | cut -d'-' -f1 | sort | uniq > $upper_log
comm -12 $lower_log $upper_log > $done_log

# Ensure the delete folder exists
mkdir -p $del

# Read each run ID from the file
while IFS= read -r run_id; do
    # Find files matching the run ID
    matching_files=($(ls $outputs | grep $run_id))

    # Check the number of matching files
    if [ ${#matching_files[@]} -ne 2 ]; then
        for file in "${matching_files[@]}"; do
            mv $outputs/$file $del/
            echo "Moved: $outputs/$file -> $del/"
        done
    fi
done < <(cat $lower_log $upper_log | sort | uniq)
