from outsource.submitter import Submitter


class SubmitterSlurm(Submitter):

    # This flag will always be True for the Slurm submitter
    user_installed_packages = True

    def __init__(
        self,
        runlist,
        context_name,
        xedocs_version,
        image,
        workflow_id=None,
        rucio_upload=False,
        rundb_update=False,
        ignore_processed=False,
        stage=False,
        remove_heavy=True,
        resources_test=False,
        debug=False,
        relay=False,
        **kwargs,
    ):
        super().__init__(
            runlist=runlist,
            context_name=context_name,
            xedocs_version=xedocs_version,
            image=image,
            workflow_id=workflow_id,
            rucio_upload=rucio_upload,
            rundb_update=rundb_update,
            ignore_processed=ignore_processed,
            stage=stage,
            remove_heavy=remove_heavy,
            resources_test=resources_test,
            debug=debug,
            relay=relay,
        )
