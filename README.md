# outsource

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/XENONnT/outsource/master.svg)](https://results.pre-commit.ci/latest/github/XENONnT/outsource/master)
[![PyPI version shields.io](https://img.shields.io/pypi/v/xe-outsource.svg)](https://pypi.python.org/pypi/xe-outsource/)

Job submission code for XENONnT.

Outsource submits XENON processing jobs to the [Open Science Grid](osg-htc.org). It is essentially a wrapper around [Pegasus](https://pegasus.isi.edu), which is itself something of a wrapper around [HTCondor](https://htcondor.readthedocs.io/en/latest/).


## Prerequisites
Those running outsource need to be production users of XENON (ie computing experts, etc). Therefore you will need certain permissions:
  - Access to the XENON OSG login node named ap23 (and a CI account). For more details see [here](https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenonnt:analysis:guide#for_ci-connect_osg_service).
  - Credentials for a production user to RunDB API
  - A grid certificate with access to the `production` rucio account.

#### Grid certificates
Production users will need their own grid certificate to transfer data on the grid. You can get a certificate via [CIlogon](https://www.cilogon.org/home).
After you get your certificate, you will need to add it to join the XENON VO. For more instructions here, see this (outdated but still useful) [wiki page](https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenon1t:sim:grid).

Additionally, you will need to add the DN of this certificate to the production rucio account. For this, please ask on slack and tag Judith or Pascal.
After you have these credentials set up, you are ready to use outsource and submit processing jobs to OSG.

#### Environment
Please use XENONnT environment. On the OSG submit hosts, this can be set up by sourcing (assuming you are on AP23):

```
#!/bin/bash

. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh
export XENON_CONFIG=$HOME/.xenon_config
export RUCIO_ACCOUNT=production
export X509_USER_PROXY=$HOME/.xenon_service_proxy
export PYTHONPATH=`pegasus-config --python`:$PYTHONPATH
```

#### Proxy
Please make sure you create a 2048 bit long key. Example:

```
voms-proxy-init -voms xenon.biggrid.nl -bits 2048 -hours 168 -out ~/.xenon_service_proxy
```

At the moment, outsource assumes that your certificate proxy is located at `X509_USER_PROXY`.

## Installation
Since outsource is used by production users, it is currently not pip-installable (people often want to change the source code locally anyway). To install, first clone the repository from github into a directory of your preference in your home directory on the xenon OSG login node.

```
git clone https://github.com/XENONnT/outsource.git
```

Outsource depends on several packages in the XENON base environment. Therefore we recommend installing from within one of those environments. We cannot use containers due to the fact we rely on the host system installation of HTCondor for job submission.
Therefore, we recommend using the standard CVMFS installation of the xenon environments, e.g.

```
. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh
```

Then you can install using pip. We recommend doing a normal (albeit user) install because there is an executable script that doesn't get installed properly in development mode.

```
cd outsource
pip install ./ --user
```

Note that if you change anything in the source code you will need to reinstall each time. If you want to install in development mode, instead (or additionally) do

```
pip install -e ./ --user
```

but note that the `outsource` executable might not pick up all your changes if you go this route.


## Configuration file

Just like [utilix](https://github.com/XENONnT/utilix), this tool expects a configuration file at environmental variable `XENON_CONFIG`. You will need to create your own config to look like below, but fill in the values.

Particularly it uses information in the field of the config with header 'Outsource', see below.

**Note you will need to fill in the empty slots**.

```
[basic]
# usually helpful for debugging but it's a lot of msg
logging_level=DEBUG

[RunDB]
rundb_api_url = <ask teamA>
rundb_api_user = xenon-admin
rundb_api_password = <ask teamA>
xent_user = nt_analysis
xent_password = <ask teamA>
xent_database = xenonnt
xe1t_url = <ask teamA>
xe1t_user = 1t_bookkeeping
xe1t_password = <ask teamA>
xe1t_database = run

[Outsource]
work_dir = /scratch/$USER/workflows
x509_user_proxy = $HOME/.xenon_service_proxy
user_install_package = strax, straxen, cutax, rucio, utilix, admix, outsource
check_user_install_package = strax, straxen, cutax, rucio, utilix, admix, outsource
# sites to exclude (GLIDEIN_Site), comma seprated list
exclude_sites = SU-ITS, NotreDame, UConn-HPC, Purdue Geddes, Chameleon, WSU-GRID, SIUE-CC-production, Lancium
# data type to process
include_data_types = peaklets, hitlets_nv, events_nv, events_mv, event_info_double, afterpulses, led_calibration
exclude_modes = tpc_noise, tpc_rn_8pmts, tpc_commissioning_pmtgain, tpc_rn_6pmts, tpc_rn_12_pmts, nVeto_LED_calibration,tpc_rn_12pmts, nVeto_LED_calibration_2
us_only = False
hs06_test_run = False
raw_records_rse = UC_OSG_USERDISK
records_rse = UC_MIDWAY_USERDISK
peaklets_rse = UC_OSG_USERDISK
events_rse = UC_MIDWAY_USERDISK
min_run_number = 1
max_run_number = 999999
max_daily = 2000
chunks_per_job = 10
rough_disk = 16000
dagman_retry = 2
dagman_static_retry = 0
dagman_maxidle = 5000
dagman_maxjobs = 5000
min_valid_hours = 48
```

## Add a setup script
For convenience, we recommend writing a simple bash script to make it easy to setup the outsource environment. Below is an example called `setup_outsource.sh`, but note you will need to change things like usernames and container.

```
#!/bin/bash

. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh
export RUCIO_ACCOUNT=production
export X509_USER_PROXY=$HOME/.xenon_service_proxy
export PATH=/opt/pegasus/current/bin:$PATH
export PYTHONPATH=`pegasus-config --python`:$PYTHONPATH
```

What this script does is
  - Source a CVMFS environment for a particular environment we are using (this will change depending on what data you want to processs)
  - Sets the rucio account to production
  - Sets the X509 user proxy location via env variable
  - Appends the path to your bin that will find the locally installed outsource executable

Then, everytime you want to submit jobs, you can setup your environment with

```
. setup_outsource.sh
```

## Submitting workflows
After installation and setting up the environment, it is time to submit jobs. The easiest way to do this is using the `outsource` executable. You can see what this script takes as input via `outsource --help`, which returns

```
[whoami@ap23 ~]$ outsource --help
usage: Outsource [-h] --context CONTEXT --xedocs_version XEDOCS_VERSION [--image IMAGE]
                 [--detector {all,tpc,muon_veto,neutron_veto}] [--workflow_id WORKFLOW_ID] [--ignore_processed] [--debug]
                 [--from NUMBER_FROM] [--to NUMBER_TO] [--run [RUN ...]] [--runlist RUNLIST] [--rucio_upload]
                 [--rundb_update] [--keep_dbtoken] [--resources_test] [--stage_out_lower] [--stage_out_combine]
                 [--stage_out_upper]

optional arguments:
  -h, --help            show this help message and exit
  --context CONTEXT     Name of context, imported from cutax.
  --xedocs_version XEDOCS_VERSION
                        global version, an argument for context.
  --image IMAGE         Singularity image. Accepts either a full path or a single name and assumes a format like this:
                        /cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:{image}
  --detector {all,tpc,muon_veto,neutron_veto}
                        Detector to focus on. If 'all' (default) will consider all three detectors. Otherwise pass a
                        single one of 'tpc', 'neutron_veto', 'muon_veto'. Pairs of detectors not yet supported.
  --workflow_id WORKFLOW_ID
                        Custom workflow_id of workflow. If not passed, inferred from today's date.
  --ignore_processed    Ignore runs that have already been processed
  --debug               Debug mode. Does not automatically submit the workflow, and jobs do not update RunDB nor upload
                        to rucio.
  --from NUMBER_FROM    Run number to start with
  --to NUMBER_TO        Run number to end with
  --run [RUN ...]       Space separated specific run_id(s) to process
  --runlist RUNLIST     Path to a runlist file
  --rucio_upload        Upload data to rucio after processing
  --rundb_update        Update RunDB after processing
  --keep_dbtoken        Do not renew .dbtoken
  --resources_test      Whether to test the resources(memory, time, storage) usage of each job
  --stage_out_lower     Whether to stage out the results of lower level processing
  --stage_out_combine   Whether to stage out the results of combine jobs
  --stage_out_upper     Whether to stage out the results of upper level processing
```

This script requires at minimum the name of context (which must reside in the cutax version installed in the environment you are in). If no other arguments are passed, this script will try to find all data that can be processed, and process it. Some inputs from the configuration file at environmental variable `XENON_CONFIG` are also used, specifically:
  - The minimum run number considered
  - The total number of runs to process at one time
  - What data types to process
  - The exclusion of different run modes
  - etc.

As a first try, pass the `--debug` flag to see how many runs outsource would try to process. It might produce a lot of output as it also prints out the list of runs and the location of workflow.

```
outsource --debug
```

If you want to narrow down the list of runs to process, you can do one of several things:
  - Pass a run or small list of runs with `--run` flag
  - Pass a path to a text file containing a newline-separated runlist that you made in some other way with the `--runlist` flag
  - Use the `--from` and/or `--to` flags to consider a range of run numbers
  - Specify things like `--detector`, the source and run mode can be controlled in file indicated by `XENON_CONFIG`, like `include_modes` and `exclude_sources`

**One super important thing to keep in mind: you also specify the singularity image to run the jobs in**. This adds a significant possibility for mistakes, as the environment you submit jobs from (and thus do this query to find what needs to be processed) might not always be the same as the one that actually tries to do the processing.
So it's super important that the image you pass with the `--image` flag corresponds to the same `base_environment` flag as the CVMFS environment you are in. Otherwise, you might run into problems of datatype hashes not matching and/or context names not being installed in the cutax version you are using.

If it is your very first time submitting a workflow, maybe try submitting a single run in debug mode:

```
outsource --run {run_number} --debug
```

This will create a pegasus workflow, which you need to use `pegasus-run` to submit yourself. Keep in mind that it will NOT upload results to rucio and update RunDB. What's more, the results will also be copied to your scratch folder in ap23 (`/scratch/$USER/...`).
