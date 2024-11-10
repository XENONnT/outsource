import os
import sys
import argparse
import json
import time
import numpy as np
from memory_profiler import memory_usage

from utilix import uconfig
from utilix.config import setup_logger
import outsource
from outsource.workflow.process import main as process_main
from outsource.workflow.process import get_chunk_number, process
from outsource.workflow.combine import main as combine_main
from outsource.workflow.combine import merge


logger = setup_logger("outsource", uconfig.get("Outsource", "logging_level", fallback="WARNING"))

parser = argparse.ArgumentParser()
parser.add_argument("run_id", type=int)
parser.add_argument("--chunks_start", type=int)
parser.add_argument("--chunks_end", type=int)
parser.add_argument("--chunks", nargs="*", type=int)
args, _ = parser.parse_known_args()

suffix = "_".join(os.environ["PEGASUS_DAG_JOB_ID"].split("_")[:-1])

if "--chunks" in sys.argv:
    only_combine = True
else:
    only_combine = False
    if args.chunks_start >= 0:
        suffix += f"_{args.chunks_start}_{args.chunks_end}"

time_usage = dict()


def wrapper(func):
    def wrapped(st, run_id, data_type, chunks):
        if not only_combine and st.is_stored(
            run_id,
            data_type,
            chunk_number=get_chunk_number(st, run_id, data_type, chunks),
        ):
            return
        time_usage[data_type] = dict()
        t0 = time.time()
        time_usage[data_type]["start"] = t0
        if only_combine:
            merge(st, run_id, data_type, chunks)
        else:
            process(st, run_id, data_type, chunks)
        t1 = time.time()
        time_usage[data_type]["end"] = t1

    return wrapped


if only_combine:
    outsource.workflow.combine.merge = wrapper(outsource.workflow.combine.merge)
    mem = memory_usage(proc=combine_main, interval=0.1, timestamps=True)
    prefix = "combine"
else:
    outsource.workflow.process.process = wrapper(outsource.workflow.process.process)
    mem = memory_usage(proc=process_main, interval=0.1, timestamps=True)
    prefix = "process"
mem = np.array(mem)


def get_sizes(directory):
    sizes = dict()
    for dirpath, dirnames, filenames in os.walk(os.path.abspath(directory), followlinks=False):
        dirpath = os.path.abspath(dirpath)
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                sizes[file_path] = os.path.getsize(file_path)
        for dirname in dirnames:
            sizes.update(get_sizes(os.path.join(dirpath, dirname)))
        sizes[dirpath] = 0
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            if os.path.isfile(file_path):
                sizes[dirpath] += sizes[os.path.join(dirpath, file_path)]
        for dirname in dirnames:
            sizes[dirpath] += sizes[os.path.join(dirpath, dirname)]
    return sizes


storage_usage = get_sizes("./")

if time_usage:
    logger.info(f"Max memory usage: {mem[:, 0].max():.1f} MB")
    logger.info(f"Max storage usage: {storage_usage[os.path.abspath('./')] / 1e6:.1f} MB")
    np.save(f"{args.run_id:06d}_{prefix}_memory_usage_{suffix}.npy", mem)
    with open(f"{args.run_id:06d}_{prefix}_time_usage_{suffix}.json", mode="w") as f:
        f.write(json.dumps(time_usage, indent=4))
    with open(f"{args.run_id:06d}_{prefix}_storage_usage_{suffix}.json", mode="w") as f:
        f.write(json.dumps(storage_usage, indent=4))
