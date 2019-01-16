#!/bin/bash
#
# This job is run on the submit host

# Arguments
run_id=$1
root_file=$2
outsource_dir=$3

set -e

my_username=`id -un`
midway_dir="/dali/lgrandi/shockley/test"

echo
echo "This script is running under user $my_username"
echo

if [[ $my_username == "rynge" ]]; then
    # testing
    mkdir -p /home/rynge/xenon/outputs/$run_id
    cp $root_file /home/rynge/xenon/outputs/$run_id/
else
    ssh -o StrictHostKeyChecking=no ershockley@dali-login.rcc.uchicago.edu "mkdir -p $midway_dir"
    scp -o StrictHostKeyChecking=no $root_file ershockley@dali-login.rcc.uchicago.edu:$midway_dir/$root_file
fi

# Update the db here! You have access to the outsource dir the workflow was 
# submitted from.
ls $outsource_dir/ 
