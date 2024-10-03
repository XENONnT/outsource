#!/bin/bash

set -e

# Extract the arguments
tar_filename=$1
outputs_dir=$2

# Sanity check: these are the files in the output directory
ls -lh $outputs_dir

# Make a temporary directory for decompressed files
mkdir -p $outputs_dir/decompressed

# Untar the output file
tar -xzf $tar_filename -C $outputs_dir/decompressed --strip-components=1
rm $tar_filename

# Check the output
echo "Checking the output"
ls -lh

# Move the outputs
mv $outputs_dir/decompressed/* $outputs_dir/

# Goodbye
echo "Done. Exiting."
exit 0
