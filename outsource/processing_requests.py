
import utilix
from toolz import groupby
from outsource.Outsource import Outsource, DEFAULT_IMAGE
import datetime

IMAGE_FORMAT = "/cvmfs/singularity.opensciencegrid.org/xenonnt/{tag}"


def unsubmitted_jobs(token=None):
    jobs = []
    #FIXME: filter requests by request time?
    requests = utilix.ProcessingRequest.find(token=token)
    for r in requests:
        # check if job already created for this request
        j = utilix.ProcessingJob.find_one(**r.index_labels, token=token)
        if j is None:
            # Job not yet created, create it and save to DB
            j = r.create_job()
            j.save(token=token)
        if j.submission_time is None:
            # Job for this request not 
            # yet submitted, add to list
            jobs.append(j)
    return jobs


def submit_workflows(jobs, wf_id=None, upload_to_rucio=True, 
                     update_db=True, force=True, debug=False, token=None):
    """
    Submit workflows for list of jobs
    Jobs are grouped by env tag and context
    A single workflow is created for each group
    """
    
    for (env, context), grp in groupby(lambda x: (x.env, x.context), jobs):
        image = IMAGE_FORMAT.format(tag=env)
        
        wf_id = f"{env}_{context}_{sum(hash(j.job_id) for j in grp)}"

        runlist = [int(j.run_id) for j in grp]
        
        workflow = Outsource(runlist, context,
                          force_rerun=force, upload_to_rucio=upload_to_rucio,
                          update_db=update_db, debug=debug,
                          image=image, wf_id=wf_id)

        workflow.submit_workflow()
        
        # Update job status
        dt = datetime.datetime.utcnow()
        for j in grp:
            j.submission_time = dt
            j.save(token=token)
