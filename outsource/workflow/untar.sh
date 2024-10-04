#!/bin/bash

set -e

# Extract the arguments
tar_filename=$1
outputs_dir=$2

# Sanity check: these are the files in the output directory
ls -lh $outputs_dir

decompressed=$outputs_dir/decompressed-${tar_filename%%.*}

# Make a temporary directory for decompressed files
mkdir -p $decompressed

# Untar the output file
tar -xzf $tar_filename -C $decompressed --strip-components=1
rm $tar_filename

# Check the output
echo "Checking the output"
ls -lh

# Move the outputs
mv $decompressed/* $outputs_dir/
rm -r $decompressed

# Goodbye
echo "Done. Exiting."
exit 0
