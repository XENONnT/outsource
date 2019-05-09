#!/bin/bash

set -e

output_file=$1
shift
# the rest of the arguments are the inputs

ls -la

for TAR in `ls *.tar.gz`; do
    tar xzf $TAR
done

# create the new output file
tar czf $output_file data

