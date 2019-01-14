#!/bin/bash

# === arguments - make sure these match Pegasus job definition ===
export run_id=$1
export zip_file=$2
export rucio_dataset=$3
export stash_gridftp_url=$4
export pax_version=$5
export send_updates=$6

start_dir=$PWD

osg_software=/cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/3.4.22/el7-x86_64
anaconda_env=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/bin
rucio_base=/cvmfs/xenon.opensciencegrid.org/software/rucio-py27/1.8.3

jobuuid=`uuidgen`

# OSG env for gfal
source $osg_software/setup.sh
export GFAL_CONFIG_DIR=$OSG_LOCATION/etc/gfal2.d
export GFAL_PLUGIN_DIR=$OSG_LOCATION/usr/lib64/gfal2-plugins/

# Rucio env
export RUCIO_HOME=$rucio_base/rucio/
export RUCIO_ACCOUNT=xenon-analysis
export PYTHONPATH=$rucio_base/lib/python2.7/site-packages:$PYTHONPATH
export PATH=$rucio_base/bin:$PATH

# set GLIDEIN_Country variable if not already
if [[ -z "$GLIDEIN_Country" ]]; then
    export GLIDEIN_Country="US"
fi

data_downloaded=0

# If data is in Rucio, find the rse to use
if [[ $rucio_dataset != "None" ]]; then

    echo "python ${start_dir}/determine_rse.py ${rucio_dataset} $GLIDEIN_Country" 
    rse=$(python ${start_dir}/determine_rse.py ${rucio_dataset} $GLIDEIN_Country)
    if [[ $? != 0 ]]; then
        # disable rucio downloading
        echo "WARNING: determine_rse.py call failed - disabling Rucio downloading"
        rucio_dataset="None"
    fi
fi

curl_moni () {

    # NO MONITORING FOR NOW
    return 0

    echo "${_condor_GLIDEIN_Site}"
    if [[ -z $GLIDEIN_ClusterId ]]; then
        GLIDEIN_ClusterId=$(cat $_CONDOR_SCRATCH_DIR/.job.ad | awk '$1=="ClusterId" {print $3}')
    fi
    if [[ -z $GLIDEIN_ProcessId ]]; then
        GLIDEIN_ProcessId=$(cat $_CONDOR_SCRATCH_DIR/.job.ad | awk '$1=="ProcId" {print $3}')
    fi
    if [[ "${_condor_GLIDEIN_Site}" == "\"ruc.ciconnect@uct2-gk.mwt2.org/condor\"" ]]; then
        export OSG_SITE_NAME="MWT2"
    fi
    if [[ "${_condor_GLIDEIN_Site}" == "\"ruc.ciconnect@iut2-gk.mwt2.org/condor\"" ]]; then
        export OSG_SITE_NAME="MWT2"
    fi
    if [[ -z $OSG_SITE_NAME ]]; then
        if [[ -n $GLIDEIN_ResourceName ]]; then
            export OSG_SITE_NAME=$GLIDEIN_ResourceName
        else
            export OSG_SITE_NAME=$HOSTNAME
        fi
        # export OSG_SITE_NAME=$HOSTNAME
    fi
    echo "Moni step $1 with ID $GLIDEIN_ClusterId"
    echo "Moni step $1 at $OSG_SITE_NAME"
    message="{\"job_id\" : \"${GLIDEIN_ClusterId}_${GLIDEIN_ProcessId}_${jobuuid}\", \"type\" : \"x1t-event\",
              \"job_progress\" : \"$1\", \"executable\" : \"run_xenon.sh\",
              \"input_filename\": \"$input_file\", \"computing_center\" : \"$OSG_SITE_NAME\",
              \"pax_version\": \"$pax_version\", \"filesize\": $filesize,
              \"storage_origin\": \"$rse\", \"timestamp\" : $(date +%s%3N)}"
    echo "Message $message"
    # curl -H "Content-Type: application/json" -X POST -I --retry 5 --retry-delay 0 --retry-max-time 20 -d "$message" http://xenon-logstash.mwt2.org:8080/
    # curl -v -s --retry 5 --retry-delay 0 --retry-max-time 20 --connect-timeout 5 -H "Content-Type: application/json" -X POST -d "$message" http://xenon-logstash.mwt2.org:8080/
    curl -v -s --retry 5 --retry-delay 0 --retry-max-time 20 --connect-timeout 5 -H "Content-Type: application/json" -X POST -d "$message" http://192.170.227.205:8080/
}

echo "start dir is $start_dir. Here's whats inside"
ls -l 

#json_file=$(ls *json)

# Pegasus has started the job in a work dir
work_dir=$PWD
export XDG_CACHE_HOME=${work_dir}/.cache
export XDG_CONFIG_HOME=${work_dir}/.config
# $XDG_DATA_DIRS
# loop and use gfal-copy before pax gets loaded to avoid
# gfal using wrong python version/libraries    

# directory to download inputs to
rawdata_path=${work_dir}/$run_id
mkdir $rawdata_path

cd ${rawdata_path}
pwd
cd ${work_dir}

(curl_moni "start downloading") || (curl_moni "start downloading")

# data download - try rucio
if [[ $rucio_dataset != "None" ]]; then
    echo "Attempting rucio download from a closeby RSE..."
    rucio_dataset_base=`echo ${rucio_dataset} | sed 's/:raw$//'`
    echo "rucio -T 18000 download ${rucio_dataset_base}:${zip_file} --no-subdir --dir ${rawdata_path} --rse ${rse}"
    download="rucio -T 18000 download ${rucio_dataset_base}:${zip_file} --no-subdir --dir ${rawdata_path} --rse ${rse} --ndownloader 1"
    echo "($download) || (sleep 60s && $download) || (sleep 120s && $download)"
    ($download) || (sleep $[ ( $RANDOM % 60 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 120 )  + 1 ]s && $download)
    if [[ $? == 0 ]]; then
        data_downloaded=1
    fi    
fi

# data download - gridftp from stash in case Rucio failed, but only in the US
if [[ $data_downloaded == 0 ]]; then
    if [[ $stash_gridftp_url != "None" && $GLIDEIN_Country == "US" ]]; then
        echo "Attempting download from Stash GridFTP..."
        download="gfal-copy -f -p -t 3600 -T 3600 -K md5 ${stash_gridftp_url} file://${rawdata_path}/${zip_file}"
        echo "($download) || (sleep 60s && $download) || (sleep 120s && $download)"
        ($download) || (sleep $[ ( $RANDOM % 60 )  + 1 ]s && $download) || (sleep $[ ( $RANDOM % 120 )  + 1 ]s && $download)
        if [[ $? == 0 ]]; then
            data_downloaded=1
        fi
    fi
fi

if [[ $data_downloaded == 0 ]]; then
    echo "ERROR: All available data sources failed. Exiting with error 25."
    exit 25
fi

(curl_moni "end downloading") || (curl_moni "end downloading")

# post-transfer, we can set up the env for pax - but first save/clear some old stuff
old_path=$PATH
export PATH=/usr/bin:/bin
old_ld_library_path=$LD_LIBRARY_PATH
unset LD_LIBRARY_PATH
old_pythonpath=$PYTHONPATH
unset PYTHONPATH

export LD_LIBRARY_PATH=$anaconda_env/lib:$LD_LIBRARY_PATH
source $anaconda_env/activate pax_v${pax_version} #_OSG
echo $PYTHONPATH

export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_${pax_version}_OSG/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/cvmfs/xenon.opensciencegrid.org/releases/anaconda/2.4/envs/pax_${pax_version}/lib:$LD_LIBRARY_PATH

mkdir $start_dir/output/
echo "output directory: ${start_dir}/output"
cd $start_dir

echo
echo
echo 'Processing...'

(curl_moni "start processing") || (curl_moni "start processing")

#echo "cax-process $1 $rawdata_path $3 $4 output $7 $8 $start_dir/${json_file}"
#cax-process $1 $rawdata_path $3 $4 output $7 $8 ${start_dir}/${json_file}

python paxify.py --input ${rawdata_path} --output output --json_path run_info.json

if [[ $? -ne 0 ]];
then 
    echo "exiting with status 25"
    exit 25
fi

pwd
ls output
echo ${start_dir}/output/$1.root

source deactivate
#export LD_LIBRARY_PATH=$old_ld_library_path

(curl_moni "end processing") || (curl_moni "end processing")

echo "---- Test line ----"
echo "Processing done, here's what's inside the output directory:"
outfile=$(ls ${start_dir}/output/)
echo "-----"
ls ${start_dir}/output/*.root
echo "-----"

