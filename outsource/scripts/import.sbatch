#!/bin/bash

#SBATCH --job-name=import
#SBATCH --time=48:00:00
#SBATCH --output=/home/%u/outsource/outsource/scripts/import_%j.log
#SBATCH --error=/home/%u/outsource/outsource/scripts/import_%j.log
#SBATCH --open-mode=append
#SBATCH --account=pi-lgrandi
#SBATCH --partition=lgrandi
#SBATCH --qos=lgrandi
#SBATCH --mem-per-cpu=16GB
#SBATCH --cpus-per-task=1

set -e

log=$1
relay=$2

workflow=$(head -n 1 $log | cut -d'/' -f7)

if [ -z $workflow ]; then
    exit 1
fi

echo "Workflow: $workflow"
echo

folder=/project/lgrandi/xenonnt/hub/$workflow

strax_data_osg=$folder/outputs/strax_data_osg
strax_data_osg_per_chunk=$folder/outputs/strax_data_osg_per_chunk

if [ -n "$(ls -A $strax_data_osg 2>/dev/null)" ] || [ -n "$(ls -A $strax_data_osg_per_chunk 2>/dev/null)" ]; then
    echo "Previous data exists. Skipping downloading."
else
    module load apptainer
    for i in 1 2 3; do
        echo "$i round downloading"
        apptainer exec --bind /project --bind /project2 /project2/lgrandi/xenonnt/singularity-images/xenonnt-el7.2025.03.1.simg \
            bash $HOME/outsource/outsource/scripts/gfal_download.sh $log $folder $relay
        echo
    done
fi

bash $HOME/outsource/outsource/scripts/rm_unfinished.sh $folder $relay
echo

bash $HOME/outsource/outsource/scripts/decompression.sh $folder
echo

echo "ALL DONE"
