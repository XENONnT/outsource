#!/bin/bash

set -e

output_file=$1
shift
# the rest of the arguments are the inputs

#source deactivate
#source activate mc
source /cvmfs/xenon.opensciencegrid.org/software/mc_old_setup.sh

#echo "hadd_mod -d -f ${proc_merged_dir}/$run.root ${proc_zip_dir}/XENON*root"
echo "hadd_mod -d -f $output_file $@"
hadd_mod -d -f $output_file $@

