#!/usr/bin/env python

"""Creates the JADE configuration for stage 2 of the demo pipeline."""

import os
import sys

from jade.models import PipelineConfig
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import load_data

PRED_GDP_COMMANDS_FILE = "pred_gdp_commands.txt"


def main():
    config = PipelineConfig(**load_data(os.environ["JADE_PIPELINE_STATUS_FILE"]))
    cur_stage = config.stages[-1]
    cur_stage_output = cur_stage.path
    previous_stage = config.stages[-2]
    previous_stage_output = previous_stage.path
    script = "jade/extensions/demo/merge_pred_gdp.py"

    with open(PRED_GDP_COMMANDS_FILE, "w") as f_out:
        cmd = f"python {script} run {previous_stage_output} {cur_stage_output}"
        f_out.write(cmd + "\n")

    cmd = "jade config create pred_gdp_commands.txt -c config-stage2.json"
    sys.exit(run_command(cmd))


if __name__ == "__main__":
    main()
