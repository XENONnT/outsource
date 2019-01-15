# outsource
Job submission code for XENONnT

## Configuration file

This tool expects a configuration file named `$HOME/.xenonnt.conf`. Note that 
environment variables can be used in the form `$HOME`. Example:

    [Common]
    
    rundb_api_url = [ask Evan]
    rundb_api_user = [ask Evan]
    rundb_api_password = [ask Evan]
    
    [Outsource]
    
    pax_version = 6.9.0
    work_dir = /scratch/$USER/workflows
    pegasus_path = /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/rhel6/4.9.0dev

    # sites to exclude (GLIDEIN_Site), comma seprated list
    exclude_sites = 
    
    
