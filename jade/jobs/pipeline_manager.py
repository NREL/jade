"""Manages the execution of a pipeline of JADE configurations."""

import logging
import os
import shutil
import time

from jade.exceptions import ExecutionError, InvalidParameter
from jade.jobs.job_submitter import JobSubmitter
from jade.models.pipeline import PipelineStage, PipelineConfig
from jade.models.submitter_params import SubmitterParams
from jade.result import Result, serialize_result
from jade.utils.subprocess_manager import run_command
from jade.utils.timing_utils import timed_info
from jade.utils.utils import dump_data, load_data


logger = logging.getLogger(__name__)


class PipelineManager:
    """Manages the execution of a pipeline of JADE configurations."""

    CONFIG_FILENAME = "pipeline.json"

    def __init__(self, config_file, output):
        self._output = output
        self._config_file = config_file
        self._config = self._deserialize()
        self._config.path = output

    @classmethod
    def create(cls, config_file, output):
        """Create a new PipelineManager.

        Parameters
        ----------
        config_file : str
        output : str
            output directory for execution

        Returns
        -------
        PipelineManager

        """
        os.makedirs(output, exist_ok=True)
        master_file = os.path.join(output, cls.CONFIG_FILENAME)
        shutil.copyfile(config_file, master_file)
        mgr = cls(master_file, output)
        for stage in mgr.stages:
            stage.path = cls.get_stage_output_path(output, stage.stage_num)
        mgr._serialize()
        return mgr

    @classmethod
    def load(cls, output):
        """Load a PipelineManager from an execution directory.

        Parameters
        ----------
        output : str

        Returns
        -------
        PipelineManager

        """
        config_file = os.path.join(output, cls.CONFIG_FILENAME)
        return cls(config_file, output)

    def submit_next_stage(self, stage_num, return_code=None):
        """Submit the next stage of the pipeline for execution.

        Parameters
        ----------
        stage_num : int
            stage number to submit
        return_code : int
            status of the previous stage if this wasn't the first

        """
        # There is a challenge in passing the information that each possible
        # stage/extension might require. This solution attempts to be as
        # flexible as possible without requiring a specific interface.
        #
        # Set these environment variables so that the auto-config scripts
        # can extract information they need to create execution script
        # arguments.
        os.environ["JADE_PIPELINE_OUTPUT_DIR"] = self._output
        os.environ["JADE_PIPELINE_STATUS_FILE"] = self._config_file
        try:
            self._submit_next_stage(stage_num, return_code=return_code)
        finally:
            os.environ.pop("JADE_PIPELINE_OUTPUT_DIR")
            os.environ.pop("JADE_PIPELINE_STATUS_FILE")
            if "JADE_PIPELINE_STAGE_ID" in os.environ:
                os.environ.pop("JADE_PIPELINE_STAGE_ID")

    @staticmethod
    def create_config(auto_config_cmds, config_file, submit_params):
        """Create a new PipelineConfig.

        Parameters
        ----------
        auto_config_cmds : list
            list of commands (str) used to create Jade configs.
        config_file : str
        submit_params : SubmitterParams

        Returns
        -------
        PipelineConfig

        """
        stages = []
        for i, cmd in enumerate(auto_config_cmds):
            stage_num = i + 1
            stages.append(
                PipelineStage(
                    auto_config_cmd=cmd,
                    config_file=PipelineManager.get_stage_config_file_name(stage_num),
                    stage_num=stage_num,
                    submitter_params=submit_params,
                )
            )

        config = PipelineConfig(stages=stages, stage_num=1)
        with open(config_file, "w") as f_out:
            f_out.write(config.json(indent=2))
        logger.info("Created pipeline config file %s", config_file)

    def _deserialize(self):
        return PipelineConfig(**load_data(self._config_file))

    def _serialize(self):
        print(self.stage_num)
        with open(self._config_file, "w") as f_out:
            f_out.write(self._config.json(indent=2))

    def _submit_next_stage(self, stage_num, return_code=None):
        if return_code is None:
            assert stage_num == 1, str(stage_num)
        else:
            if stage_num != self.stage_num + 1:
                raise InvalidParameter(
                    f"expected stage_num {self.stage_num + 1}, received {stage_num}"
                )

            self._config.stages[stage_num - 2].return_code = return_code
            self._config.stage_num += 1

        if self._config.stage_num == len(self._config.stages) + 1:
            logger.info("Pipeline is complete")
            self._config.is_complete = True
            self._serialize()
            return

        logger.info("Start execution pipeline stage %s/%s", stage_num, len(self._config.stages))

        self._serialize()
        stage = self._config.stages[self.stage_num - 1]
        os.environ["JADE_PIPELINE_STAGE_ID"] = str(self.stage_num)
        self._run_auto_config(stage)
        output = self.get_stage_output_path(self.path, self.stage_num)
        ret = JobSubmitter.run_submit_jobs(
            stage.config_file,
            output,
            stage.submitter_params,
            pipeline_stage_num=self.stage_num,
        )
        if ret != 0:
            raise ExecutionError(f"stage {self.stage_num} failed")

    def _run_auto_config(self, stage):
        if os.path.exists(stage.config_file):
            os.remove(stage.config_file)

        ret = run_command(stage.auto_config_cmd)
        if ret != 0:
            raise ExecutionError(f"Failed to auto-config stage {self.stage_num}: {ret}")

        if not os.path.exists(stage.config_file):
            raise ExecutionError(
                f"auto-config stage {self.stage_num} did not produce {stage.config_file}"
            )

        final_file = self.get_stage_config_file_path(self._output, self.stage_num)
        shutil.copyfile(stage.config_file, final_file)
        stage.config_file = final_file

    @property
    def config(self):
        """Return the pipeline config.

        Returns
        -------
        PipelineConfig

        """
        return self._config

    @property
    def path(self):
        """Return the pipeline directory.

        Returns
        -------
        int

        """
        return self._config.path

    @property
    def stage_num(self):
        """Return the current stage index

        Returns
        -------
        int

        """
        return self._config.stage_num

    @property
    def stages(self):
        """Return the stages in the pipeline.

        Returns
        -------
        list
            list of PipelineStage

        """
        return self._config.stages

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
