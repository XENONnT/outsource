#!/bin/bash

set -e

# Extract the arguments
tar_filename=$1
outputs_dir=$2

# Sanity check: these are the files in the output directory
ls -lh $outputs_dir

# Make a temporary directory for decompressed files
mkdir $outputs_dir/decompressed

# Untar the output file
tar -xzf $outputs_dir/$tar_filename -C decompressed
rm $outputs_dir/$tar_filename

# Check the output
echo "Checking the output"
ls -lh

# Move the outputs
mv decompressed/* $outputs_dir/

# Goodbye
echo "Done. Exiting."
exit 0
