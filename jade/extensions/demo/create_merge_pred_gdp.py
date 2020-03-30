#!/usr/bin/env python

"""Creates the JADE configuration for stage 2 of the demo pipeline."""

import os
import sys

from jade.utils.subprocess_manager import run_command
from jade.utils.utils import load_data


def main():
    status = load_data(os.environ["JADE_PIPELINE_STATUS_FILE"])
    cur_stage = status["stages"][-1]
    cur_stage_output = cur_stage["output_directory"]
    previous_stage = status["stages"][-2]
    previous_stage_output = previous_stage["output_directory"]
    script = "jade/extensions/demo/merge_pred_gdp.py"

    with open("pred_gdp_commands.txt", "w") as f_out:
        cmd = f"python {script} run {previous_stage_output} {cur_stage_output}"
        f_out.write(cmd + "\n")

    cmd = "jade auto-config generic_command pred_gdp_commands.txt -c config-stage2.json"
    sys.exit(run_command(cmd))


if __name__ == "__main__":
    main()
