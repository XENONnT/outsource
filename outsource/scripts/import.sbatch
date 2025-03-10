#!/bin/bash

#SBATCH --job-name=import
#SBATCH --time=24:00:00
#SBATCH --output=/home/%u/outsource/outsource/scripts/import_%j.log
#SBATCH --error=/home/%u/outsource/outsource/scripts/import_%j.log
#SBATCH --open-mode=append
#SBATCH --account=pi-lgrandi
#SBATCH --partition=caslake
#SBATCH --qos=caslake
#SBATCH --mem-per-cpu=8GB
#SBATCH --cpus-per-task=1

set -e

log=$1
relay=$2

workflow=$(head -n 1 $log | cut -d'/' -f7)

if [ -z $workflow ]; then
    exit 1
fi

module load apptainer
for i in 1 2 3; do
    echo "$i round downloading"
    apptainer exec --bind /project --bind /project2 /project2/lgrandi/xenonnt/singularity-images/xenonnt-el7.2025.03.1.simg \
        bash $HOME/outsource/outsource/scripts/gfal_download.sh $log /project/lgrandi/xenonnt/hub/$workflow $relay
    echo
done

bash $HOME/outsource/outsource/scripts/rm_unfinished.sh /project/lgrandi/xenonnt/hub/$workflow $relay
echo

bash $HOME/outsource/outsource/scripts/decompression.sh /project/lgrandi/xenonnt/hub/$workflow
echo
