# outsource
Job submission code for XENONnT

## Configuration file

This tool expects a configuration file named `$HOME/.xenonnt.conf`. Note that 
environment variables can be used in the form `$HOME`. Example:

    [Common]
    
    rundb_api_url = 
    rundb_api_user = 
    rundb_api_key = 
    
    [Outsource]
    
    pax_version = 6.9.0
    work_dir = /scratch/$USER/workflows
    pegasus_path = /cvmfs/oasis.opensciencegrid.org/osg/projects/pegasus/rhel6/4.9.0dev
    
    