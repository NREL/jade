"""Manages the execution of a pipeline of JADE configurations."""

import logging
import os
import shutil
import time

from jade.exceptions import ExecutionError
from jade.models.submitter_options import DEFAULTS
from jade.result import Result, serialize_result
from jade.utils.subprocess_manager import run_command
from jade.utils.timing_utils import timed_info
from jade.utils.utils import dump_data, load_data


logger = logging.getLogger(__name__)


class PipelineManager:
    """Manages the execution of a pipeline of JADE configurations."""

    CONFIG_FILENAME = "pipeline.toml"
    STATUS_FILENAME = "pipeline_status.toml"

    def __init__(self, config_file, output):
        self._output = output
        os.makedirs(self._output, exist_ok=True)
        self._config_file = os.path.join(self._output, self.CONFIG_FILENAME)
        shutil.copyfile(config_file, self._config_file)
        self._config = load_data(self._config_file)
        self._stages = self._config["stages"]
        self._status_file = os.path.join(self._output, self.STATUS_FILENAME)
        self._cur_stage_id = 1
        self._status_info = {
            "current_stage_id": self._cur_stage_id,
            "stages": [],
            "output_directory": self._output,
            "config_file": self._config_file,
        }

    @staticmethod
    def create(auto_config_cmds, config_file, submit_params=None):
        """Create a pipeline with multiple Jade configurations."""
        data = {"stages": []}

        user_submit_params = {}
        if submit_params:
            for option in submit_params.split(" "):
                if "=" in option:
                    param, value = option.split("=")
                else:
                    param, value = option, ""

                if param in ["-b", "--per-node-batch-size"]:
                    user_submit_params[param] = int(value)
                elif param in ["-n", "--max-nodes"]:
                    user_submit_params[param] = int(value)
                elif param in ["-q", "--num-processes"]:
                    user_submit_params[param] = int(value)
                elif param in ["-p", "--poll-interval"]:
                    user_submit_params[param] = float(value)
                else:
                    user_submit_params[param] = value

        for i, cmd in enumerate(auto_config_cmds):
            stage_num = i + 1
            data["stages"].append(
                {
                    "auto_config_cmd": cmd,
                    "config_file": PipelineManager.get_stage_config_file_name(stage_num),
                    "submit-params": {
                        "--max-nodes": DEFAULTS["max_nodes"],
                        "--per-node-batch-size": DEFAULTS["per_node_batch_size"],
                        "--num-processes": None,
                    }
                }
            )
            data["stages"][-1]["submit-params"].update(user_submit_params)

        dump_data(data, config_file)
        logger.info("Created pipeline config file %s", config_file)

    @timed_info
    def submit(self, verbose=False):
        """Submit the pipeline for execution."""
        logger.info("Start execution pipeline %s num_stages=%s",
                    self._config_file, len(self._stages))

        # There is a challenge in passing the information that each possible
        # stage/extension might require. This solution attempts to be as
        # flexible as possible without requiring a specific interface.
        #
        # Set these environment variables so that the auto-config scripts
        # can extract information they need to create execution script
        # arguments.
        os.environ["JADE_PIPELINE_OUTPUT_DIR"] = self._output
        os.environ["JADE_PIPELINE_STATUS_FILE"] = self._status_file
        try:
            self._submit(verbose)
        finally:
            os.environ.pop("JADE_PIPELINE_OUTPUT_DIR")
            os.environ.pop("JADE_PIPELINE_STATUS_FILE")
            if "JADE_PIPELINE_STAGE_ID" in os.environ:
                os.environ.pop("JADE_PIPELINE_STAGE_ID")

    def _submit(self, verbose):
        for stage in self._stages:
            os.environ["JADE_PIPELINE_STAGE_ID"] = str(self._cur_stage_id)
            stage_info = {
                "stage_id": self._cur_stage_id,
                "output_directory": self.get_stage_output_path(
                    self._output,
                    self._cur_stage_id
                )
            }
            self._status_info["stages"].append(stage_info)
            self._dump_status()
            self._run_auto_config(stage)
            cmd = self._make_submit_cmd(stage, verbose)
            start = time.time()
            ret = run_command(cmd)
            end = time.time()
            exec_time = end - start
            result = Result(str(self._cur_stage_id), ret, "finished", exec_time, end)
            self._status_info["stages"][-1]["result"] = serialize_result(result)
            self._dump_status()
            if ret != 0:
                raise ExecutionError(f"stage {self._cur_stage_id} failed")
            self._cur_stage_id += 1

        logger.info("Finished execution pipeline")

    def _dump_status(self):
        self._status_info["current_stage_id"] = self._cur_stage_id
        dump_data(self._status_info, self._status_file)

    def _run_auto_config(self, stage):
        config_file = stage["config_file"]
        if os.path.exists(config_file):
            os.remove(config_file)

        auto_config_cmd = stage["auto_config_cmd"]
        ret = run_command(auto_config_cmd)
        if ret != 0:
            raise ExecutionError(
                f"Failed to auto-config stage {self._cur_stage_id}: {ret}"
            )

        if not os.path.exists(config_file):
            raise ExecutionError(
                f"auto-config stage {self._cur_stage_id} did not produce {config_file}"
            )

        final_file = self.get_stage_config_file_path(self._output, self._cur_stage_id)
        shutil.move(config_file, final_file)
        stage["config_file"] = final_file

    def _make_submit_cmd(self, stage, verbose):
        stage_output = self.get_stage_output_path(self._output, self._cur_stage_id)
        cmd = f"jade submit-jobs {stage['config_file']} -o {stage_output}"
        for key, val in stage["submit-params"].items():
            if val == "":
                cmd += f" {key}"
            else:
                cmd += f" {key}={val}"
        if verbose:
            cmd += " --verbose"

        return cmd

    @staticmethod
    def get_stage_config_file_name(stage_num):
        """Return the filename of a stage config file."""
        return f"config-stage{stage_num}.json"

    @staticmethod
    def get_stage_config_file_path(output, stage_num):
        """Return the path to a stage config file."""
        return os.path.join(output, PipelineManager.get_stage_config_file_name(stage_num))

    @staticmethod
    def get_stage_output_name(stage_num):
        """Return the output directory name of a stage."""
        return f"output-stage{stage_num}"

    @staticmethod
    def get_stage_output_path(output, stage_num):
        """Return the path to the output directory of a stage."""
        return os.path.join(output, PipelineManager.get_stage_output_name(stage_num))
