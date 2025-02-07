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
workflow_path=/ospool/uc-shared/project/xenon/$USER/workflows/$1/runs

# list the combine jobs for lower level
combine=$(tree -if $workflow_path | grep combine | awk -F'/' '{print $(NF-2) "/" $(NF-1)}' | sort | uniq)
for path in $combine; do
    recursive_ls $remote_path/runs/$path 2>/dev/null
done

# list the upper jobs for upper level
upper=$(tree -if $workflow_path | grep upper | awk -F'/' '{print $(NF-2) "/" $(NF-1)}' | sort | uniq)
for path in $upper; do
    recursive_ls $remote_path/runs/$path 2>/dev/null
done
