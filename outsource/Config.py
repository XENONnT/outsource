import os
from utilix.config import Config

config = Config()

dir_raw = "/xenon/xenon1t/raw"

base_dir = os.path.abspath(os.path.dirname(__file__))
work_dir = config.get("Outsource", "work_dir")
runs_dir = os.path.join(work_dir, "runs")
pegasus_path = config.get("Outsource", "pegasus_path")
