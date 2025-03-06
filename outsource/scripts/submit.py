import os
import argparse
import numpy as np
from utilix import xent_collection, uconfig, DB
from utilix.io import load_runlist
from utilix.config import setup_logger

from outsource.utils import get_context, get_runlist


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))
coll = xent_collection()


def main():
    parser = argparse.ArgumentParser("Outsource")
    parser.add_argument("--context", required=True, help="Name of context, imported from cutax.")
    parser.add_argument(
        "--xedocs_version", required=True, help="global version, an argument for context."
    )
    parser.add_argument(
        "--image",
        help=(
            "Singularity image. Accepts either a full path or a single name "
            "and assumes a format like this: "
            "/cvmfs/singularity.opensciencegrid.org/xenonnt/base-environment:{image}"
        ),
    )
    parser.add_argument(
        "--detector",
        default="all",
        help=(
            "Detector to focus on. If 'all' (default) will consider all three detectors. "
            "Otherwise pass a single one of 'tpc', 'neutron_veto', 'muon_veto'. "
            "Pairs of detectors not yet supported. "
        ),
        choices=["all", "tpc", "muon_veto", "neutron_veto"],
    )
    parser.add_argument(
        "--workflow_id",
        help="Custom workflow_id of workflow. If not passed, inferred from today's date.",
    )
    parser.add_argument(
        "--ignore_processed",
        dest="ignore_processed",
        action="store_true",
        help="Ignore runs that have already been processed",
    )
    parser.add_argument(
        "--stage",
        dest="stage",
        action="store_true",
        help="Stage data before downloading",
    )
    parser.add_argument(
        "--remove_heavy",
        dest="remove_heavy",
        action="store_true",
        help="Remove heavy chunk immediately after processing",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help=(
            "Debug mode. Does not automatically submit the workflow, "
            "and jobs do not update RunDB nor upload to rucio."
        ),
    )
    parser.add_argument("--from", dest="number_from", type=int, help="Run number to start with")
    parser.add_argument("--to", dest="number_to", type=int, help="Run number to end with")
    parser.add_argument(
        "--run", nargs="*", type=int, help="Space separated specific run_id(s) to process"
    )
    parser.add_argument("--runlist", type=str, help="Path to a runlist file")
    parser.add_argument(
        "--rucio_upload",
        dest="rucio_upload",
        action="store_true",
        help="Upload data to rucio after processing",
    )
    parser.add_argument(
        "--rundb_update",
        dest="rundb_update",
        action="store_true",
        help="Update RunDB after processing",
    )
    parser.add_argument(
        "--keep_dbtoken",
        dest="keep_dbtoken",
        action="store_true",
        help="Do not renew .dbtoken",
    )
    parser.add_argument(
        "--resources_test",
        dest="resources_test",
        action="store_true",
        help="Whether to test the resources(memory, time, storage) usage of each job",
    )
    parser.add_argument(
        "--stage_out_lower",
        dest="stage_out_lower",
        action="store_true",
        help="Whether to stage out the results of lower level processing",
    )
    parser.add_argument(
        "--stage_out_combine",
        dest="stage_out_combine",
        action="store_true",
        help="Whether to stage out the results of combine jobs",
    )
    parser.add_argument(
        "--stage_out_upper",
        dest="stage_out_upper",
        action="store_true",
        help="Whether to stage out the results of upper level processing",
    )
    parser.add_argument(
        "--resubmit",
        dest="resubmit",
        action="store_true",
        help="Resubmit the workflow for the runlist",
    )
    parser.add_argument(
        "--relay",
        dest="relay",
        action="store_true",
        help="Whether in OSG-RCC relay mode",
    )
    # Exclusive groups on submission destination
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--htcondor", action="store_true", help="Submission for htcondor")
    group.add_argument("--slurm", action="store_true", help="Submission for slurm")
    group.add_argument(
        "--osg_rcc",
        dest="osg_rcc",
        action="store_true",
        help="Submission for slurm, but in OSG-RCC relay mode",
    )
    args = parser.parse_args()

    if not args.keep_dbtoken:
        dbtoken = os.path.join(os.environ["HOME"], ".dbtoken")
        if os.path.exists(dbtoken):
            os.remove(dbtoken)
        # Remove the cached DB instance and reinitialize it
        DB._instances = dict()
        DB()

    if "development" in args.image:
        raise RuntimeError("Cannot use development images/container for processing!")

    if args.ignore_processed and args.rucio_upload:
        raise RuntimeError("Cannot upload to rucio in debug mode.")

    if not args.rucio_upload and args.rundb_update:
        raise RuntimeError("Cannot update RunDB without uploading to rucio.")

    st = get_context(args.context, args.xedocs_version)

    if args.run and args.runlist:
        raise RuntimeError("Cannot pass both --run and --runlist. Please choose one.")

    if args.run:
        _runlist = args.run
    elif args.runlist:
        _runlist = load_runlist(args.runlist)
    else:
        _runlist = []

    if args.htcondor:
        from outsource.submitters.htcondor import SubmitterHTCondor

        submitter_class = SubmitterHTCondor
    elif args.slurm:
        from outsource.submitters.slurm import SubmitterSlurm

        submitter_class = SubmitterSlurm
    elif args.osg_rcc:
        if not args.relay:
            raise ValueError(
                "OSG-RCC mode must be used with --relay flag. Please specify --osg_rcc --relay"
            )
        from outsource.submitters.relay import SubmitterRelay

        submitter_class = SubmitterRelay
    else:
        raise ValueError(
            "No submission destination specified. "
            "Please specify one of the following: --slurm, --htcondor, --osg_rcc"
        )

    if not args.osg_rcc:
        runlist = get_runlist(
            st,
            detector=args.detector,
            runlist=_runlist,
            number_from=args.number_from,
            number_to=args.number_to,
            ignore_processed=args.ignore_processed,
        )

        max_num = uconfig.getint("Outsource", "max_num", fallback=None)
        if max_num:
            rng = np.random.default_rng(seed=max_num)
            runlist = sorted(
                rng.choice(runlist, min(max_num, len(runlist)), replace=False).tolist()
            )
        logger.info(f"The following {len(runlist)} runs will be processed: {sorted(runlist)}")
        missing_runlist = set(_runlist) - set(runlist)
        if missing_runlist:
            logger.warning(
                f"The following {len(missing_runlist)} run_ids will not be processed "
                f"after checking dependeicies in the RunDB: {sorted(missing_runlist)}"
            )
        if not runlist:
            raise RuntimeError(
                "Cannot find any runs matching the criteria "
                "specified in your input and XENON_CONFIG!"
            )
    else:
        runlist = _runlist

    # This object contains all the information needed to submit the workflow
    submitter = submitter_class(
        runlist,
        context_name=args.context,
        xedocs_version=args.xedocs_version,
        image=args.image,
        workflow_id=args.workflow_id,
        rucio_upload=args.rucio_upload,
        rundb_update=args.rundb_update,
        ignore_processed=args.ignore_processed,
        stage=args.stage,
        remove_heavy=args.remove_heavy,
        resources_test=args.resources_test,
        stage_out_lower=args.stage_out_lower,
        stage_out_combine=args.stage_out_combine,
        stage_out_upper=args.stage_out_upper,
        debug=args.debug,
        relay=args.relay,
        resubmit=args.resubmit,
    )

    # Finally submit the workflow
    submitter.submit()


if __name__ == "__main__":
    main()
