#!/bin/bash

#SBATCH --job-name=import
#SBATCH --time=48:00:00
#SBATCH --output=/project/lgrandi/xudc/analysiscode/straxen_debug/bash_scripts/import_%j.log
#SBATCH --open-mode=append
#SBATCH --account=pi-lgrandi
#SBATCH --partition=caslake
#SBATCH --qos=caslake
#SBATCH --mem=16G

set -e

datetime=$1
relay=$2

for i in 1 2 3; do
    echo '$i round downloading'
    bash /home/$USER/outsource/outsource/scripts/gfal_download.sh /home/$USER/gfal_ls_recursive_$datetime.log /project/lgrandi/xenonnt/hub/user-el9.2025.02.2-xenonnt_offline-global_v17-$datetime
done

bash /home/$USER/outsource/outsource/scripts/rm_unfinished.sh /project/lgrandi/xenonnt/hub/user-el9.2025.02.2-xenonnt_offline-global_v17-$datetime $relay

bash /home/$USER/outsource/outsource/scripts/decompression.sh /project/lgrandi/xenonnt/hub/user-el9.2025.02.2-xenonnt_offline-global_v17-$datetime
