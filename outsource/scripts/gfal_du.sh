#!/bin/bash

set -e

export PYTHONWARNINGS="ignore::DeprecationWarning"

remote_path=davs://xenon-gridftp.grid.uchicago.edu:2880/xenon/scratch/$USER/$1

total_size=0

get_size_recursive() {
    local path=$1
    echo $path
    # gfal-ls $path | while read -r item; do
    while read -r item; do
        if [[ $path == $item ]]; then
            size=$(gfal-ls -l $path | awk '{print $5}')
            total_size=$((total_size + size))
        else
            full=$path/$item
            if gfal-ls $full &>/dev/null; then
                get_size_recursive $full
            else
                exit 1
            fi
        fi
    done < <(gfal-ls "$path")
}

get_size_recursive $remote_path

echo "Total size: $total_size bytes"
echo "In GB: $(awk -v s="$total_size" 'BEGIN { printf "%.2f GB\n", s/1024/1024/1024 }')"
