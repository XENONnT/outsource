# outsource
Job submission code for XENONnT

## Installation
TODO

Outsource requires modules from [utilix](https://github.com/XENONnT/utilix)

## Configuration file

Just like utilix, this tool expects a configuration file named `$HOME/.xenonnt.conf`. Particularly it uses information in the field of the config with header 'Outsource', see below: 

    [RunDB]
    
    rundb_api_url = [ask Evan]
    rundb_api_user = [ask Evan]
    rundb_api_password = [ask Evan]
    
    [Outsource]
    
    pax_version = 6.9.0
    work_dir = /scratch/$USER/workflows
    pegasus_path = /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/rhel6/4.9.0dev

    # sites to exclude (GLIDEIN_Site), comma seprated list
    exclude_sites = 
    
    
