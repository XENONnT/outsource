# outsource
Job submission code for XENONnT. 

Outsource submits XENON processing jobs to the [Open 
Science Grid](osg-htc.org). It is essentially a wrapper 
around [Pegasus](https://pegasus.isi.edu), which is 
itself something of a wrapper around 
[HTCondor DAGMan](https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html).


## Prerequisites
Those running outsource need to be production users of 
XENON (ie computing experts, etc). Therefore you will 
need certain permissions:
  - Access to the XENON OSG login node (and a CI account). 
    For more details see [here](xenon:xenon1t:cmp:computing:midway_cluster:instructions).
  - Credentials for a production user to RunDB API
  - A grid certificate with access to the `production` 
    rucio account.
    
#### Grid certificates
Production users will need their own grid certificate to 
transfer data on the grid. You can get a certificate via 
[CIlogon](https://www.cilogon.org/home). After you get your certificate, you will need to add it 
to join the XENON VO. For more instructions here, see this 
(outdated but still useful) [wiki page](https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenon1t:sim:grid).
Additionally, you will need to add the DN of this 
certificate to the production rucio account. For this, 
please ask on slack and tag Judith or Pascal. 

After you have these credentials set up, you are ready 
to use outsource and submit processing jobs to OSG. 

#### Environment
Please use the Python3.6 XENONnT environment. On the OSG submit hosts, this can be set up by sourcing (assuming you are on AP23):

    #!/bin/bash
    . /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh
    export XENON_CONFIG=$HOME/.xenon_config
    export RUCIO_ACCOUNT=production
    export X509_USER_PROXY=$HOME/user_cert
    export PATH=/opt/pegasus/current/bin:$PATH
    export PYTHONPATH=`pegasus-config --python`:$PYTHONPATH
    export PYTHONPATH="$HOME/.local/lib/python3.9/site-packages:$PYTHONPATH"

#### Proxy
Please make sure you create a 2048 bit long key. Example:

    voms-proxy-init -voms xenon.biggrid.nl -bits 2048 -hours 168 -out ~/user_cert

At the moment, outsource assumes that your certificate 
proxy is located at `~/user_cert`.

## Installation
Since outsource is used by production users, it is 
currently not pip-installable (people often want to 
change the source code locally anyway). To install, 
first clone the repository from github into a directory 
of your preference in
your home 
directory on the xenon OSG login node.

``` 
git clone https://github.com/XENONnT/outsource.git
```

Outsource depends on several packages in the XENON base 
environment. Therefore we recommend installing from 
within one of those environments. We cannot use 
containers due to the fact we rely on 
the host system installation of HTCondor for job 
submission. Therefore, we recommend using the standard 
CVMFS installation of the xenon environments, e.g.
``` 
. /cvmfs/xenon.opensciencegrid.org/releases/nT/development/setup.sh
```

Then you can install using pip. We recommend doing a normal 
(albeit user) install because there is an executable 
script that doesn't get installed properly in 
development mode. 

```
cd outsource 
pip install ./ --user 
```
Note that if you change anything in the source code you 
will need to reinstall each time. If you want to install 
in development mode, instead (or additionally) do
``` 
pip install -e ./ --user
```
but note that the `outsource` executable might not pick 
up all your changes if you go this route. 


## Configuration file

Just like [utilix](https://github.com/XENONnT/utilix), 
this tool expects a configuration file named `$HOME/.
xenon_config`. You will need to create your own config 
to look like below, but fill in the values.

Particularly it uses information in the 
field of the config with header 'Outsource', see below. 
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
pegasus_path = /usr
# sites to exclude (GLIDEIN_Site), comma seprated list
exclude_sites = SU-ITS, NotreDame, UConn-HPC, Purdue Geddes, Chameleon, WSU-GRID, SIUE-CC-production, Lancium
# data type to process
dtypes = peaklets, hitlets_nv, events_nv, events_mv, event_info_double, afterpulses, led_calibration
# below are specific dtype options
raw_records_rse = UC_OSG_USERDISK
records_rse = UC_MIDWAY_USERDISK
peaklets_rse = UC_OSG_USERDISK
events_rse = UC_MIDWAY_USERDISK
exclude_modes = tpc_noise, tpc_rn_8pmts, tpc_commissioning_pmtgain, tpc_rn_6pmts, tpc_rn_12_pmts, nVeto_LED_calibration,tpc_rn_12pmts, nVeto_LED_calibration_2
use_xsede = False
notification_email = 
min_run_number = 666
max_daily = 2000
hs06_test_run = False
this_site_only =
chunks_per_job = 10
combine_memory = 60000   # MB
combine_disk = 120000000 # KB
peaklets_memory = 14500  # MB
peaklets_disk = 50000000 # KB
events_memory = 60000    # MB
events_disk = 120000000  # KB
us_only = False
```

## Add a setup script
For convenience, we recommend writing a simple bash 
script to make it easy to setup the outsource 
environment. Below is an example called `setup_outsource.
sh`, but note you will need to change things like usernames and container.

``` 
#!/bin/bash

. /cvmfs/xenon.opensciencegrid.org/releases/nT/2023.11.1/setup.sh

export RUCIO_ACCOUNT=production
export X509_USER_PROXY=$HOME/user_cert
export PATH=/opt/pegasus/current/bin:$PATH
export PYTHONPATH=`pegasus-config --python`:$PYTHONPATH
```

What this script does is
  - Source a CVMFS environment for a particular 
       environment we are using (this will change depending 
       on what data you want to processs
  - Sets the rucio 
  account to production
  - Sets the X509 user proxy 
    location via env variable
  - Appends the path to your 
    bin that will find the 
locally installed outsource executable.

Then, everytime you want to submit jobs, you can setup 
your environment with 
``` 
. setup_outsource.sh
```

## Submitting workflows
After installation and setting up the environment, it is 
time to submit jobs. The easiest way to do this is using 
the `outsource` executable. You can see what this script 
takes as input via `outsource --help`, which returns

``` 
(XENONnT_2022.06.2) [ershockley@login-el7 ~]$ outsource --help
usage: Outsource [-h] --context CONTEXT [--debug] [--name NAME] [--image IMAGE]
                 [--force] [--dry-run] [--detector DETECTOR] [--from NUMBER_FROM]
                 [--to NUMBER_TO] [--mode [MODE [MODE ...]]] [--run [RUN [RUN ...]]]
                 [--runlist RUNLIST] [--source [SOURCE [SOURCE ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --context CONTEXT     Name of context, imported from cutax.
  --debug
  --name NAME           Custom name of workflow directory. If not passed, inferred from
                        today's date
  --image IMAGE         Singularity image. Accepts either a full path or a single name
                        and assumes a format like this:
                        /cvmfs/singularity.opensciencegrid.org/xenonnt/base-
                        environment:{image}
  --force
  --dry-run
  --detector DETECTOR
  --from NUMBER_FROM    Run number to start with
  --to NUMBER_TO        Run number to end with
  --mode [MODE [MODE ...]]
                        Space separated run mode(s) to consider.
  --run [RUN [RUN ...]]
                        space separated specific run number(s) to process
  --runlist RUNLIST     Path to a runlist file
  --source [SOURCE [SOURCE ...]]
                        Space separated source(s) to consider
```

This script requires at minimum 
the 
name of context (which must reside in the cutax version 
installed in the environment you are in). If no other 
arguments are passed, this script will try to find all 
data that can be processed, and process it. Some inputs 
from the configuration file `~/.xenon_config` are also 
used, 
specifically:
  - The minimum run number considered
  - The total number of runs to process at one time
  - What data types to process
  - The exclusion of different run modes
  - etc.

As a first try, pass the `--dry-run` flag to see how 
many runs outsource would try to process. It might 
produce a lot of output as it also prints out the list 
of runs. 

``` 
outsource --dry-run
```

If you want to narrow down the list of runs to process, 
you can do one of several things:
  - Pass a run or small list of runs with `--run` flag
  - Pass a path to a text file containing a 
    newline-separated runlist that you made in some 
    other way with the `--runlist` flag. 
  - Use the `--from` and/or `--to` flags to consider a 
    range of run numbers
  - Specify things like `--mode`, `--detector`, `--source`. 

**One super important thing to keep in mind: you also 
specify the singularity image to run the jobs in**. This 
adds a significant possibility for mistakes, as the 
environment you submit jobs from (and thus do this query 
to find what needs to be processed) might not always be 
the same as the one that actually tries to do the 
processing. So it's super important that the image you 
pass with the `--image` flag corresponds to the same 
base_environment flag as the CVMFS environment you are 
in. Otherwise, you might run into problems of datatype 
hashes not matching and/or context names not being 
installed in the cutax version you are using. A nice 
feature would be to automatically get the tag from the 
environment itself so you don't have this problem (TODO: 
can someone work on 
it?). 

If it is your very first time submitting a workflow, 
maybe try submitting a single run in debug mode: 

``` 
outsource --run {run_number} --debug
```

This will create a pegasus workflow, which you need to use `pegasus-run` to submit yourself. Keep in mind that it will NOT upload results to rucio and update RunDB. What's more, the results will also be copied to your scratch folder in ap23 (`/scratch/$USER/...`). 
