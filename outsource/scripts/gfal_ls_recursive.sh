#!/bin/bash

set -e

# the core function to list the files recursively
recursive_ls() {
    local path=$1
    gfal-ls $path | while read -r item; do
        if [[ $path == $item ]]; then
            if [[ $item == *-output.tar.gz ]]; then
                echo $item
            fi
        else
            full=$path/$item
            if gfal-ls $full &>/dev/null; then
                recursive_ls $full
            else
                exit 1
            fi
        fi
    done
}

remote_path=davs://xenon-gridftp.grid.uchicago.edu:2880/xenon/scratch/$USER/$1
recursive_ls $remote_path 2>/dev/null
