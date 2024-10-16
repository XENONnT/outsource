import argparse
from utilix import xent_collection, uconfig
from utilix.io import load_runlist
from utilix.config import setup_logger

from outsource.utils import get_context, get_runlist
from outsource.submitter import Submitter


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
        "--local_transfer",
        dest="local_transfer",
        action="store_true",
        help="Transfer data to local after processing",
    )
    args = parser.parse_args()

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
        _runlist = None

    runlist = get_runlist(
        st,
        detector=args.detector,
        runlist=_runlist,
        number_from=args.number_from,
        number_to=args.number_to,
        ignore_processed=args.ignore_processed,
    )
    if set(_runlist) - set(runlist):
        logger.warning(
            "The following run_ids were not processible "
            f"after checking dependeicies in the RunDB: {sorted(set(_runlist) - set(runlist))}"
        )
    if not runlist:
        raise RuntimeError(
            "Cannot find any runs matching the criteria specified in your input and XENON_CONFIG!"
        )

    # This object contains all the information needed to submit the workflow
    submitter = Submitter(
        runlist,
        context_name=args.context,
        xedocs_version=args.xedocs_version,
        image=args.image,
        workflow_id=args.workflow_id,
        rucio_upload=args.rucio_upload,
        rundb_update=args.rundb_update,
        ignore_processed=args.ignore_processed,
        local_transfer=args.local_transfer,
        debug=args.debug,
    )

    # Finally submit the workflow
    submitter.submit()


if __name__ == "__main__":
    main()
