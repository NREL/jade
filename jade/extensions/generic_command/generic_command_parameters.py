"""Implements the JobParametersInterface for generic_command."""

import logging
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Optional, Set

from pydantic import Field, validator

from jade.models import JadeBaseModel
from jade.models.spark import SparkConfigModel, SparkContainerModel
from jade.common import DEFAULT_SUBMISSION_GROUP
from jade.jobs.job_parameters_interface import JobParametersInterface


logger = logging.getLogger(__name__)

_EXTENSION = "generic_command"


class GenericCommandParameters(JobParametersInterface):
    """A class used for creating a job for a generic command."""

    parameters_type = namedtuple("GenericCommand", "command")

    def __init__(self, **kwargs):
        self._model = GenericCommandParametersModel(**kwargs)

    def __str__(self):
        return "<GenericCommandParameters: {}>".format(self.name)

    def __getattr__(self, name):
        if name in GenericCommandParametersModel.__fields__:
            return getattr(self._model, name)
        raise AttributeError(f"'GenericCommandParameters' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name == "extension":
            raise AttributeError(f"'GenericCommandParameters' does not allow setting 'extension'")
        if name in GenericCommandParametersModel.__fields__:
            setattr(self._model, name, value)
        self.__dict__[name] = value

    @property
    def command(self):
        if self._model.use_multi_node_manager:
            return f"jade-internal run-multi-node-job {self.name} {self._model.command}"
        elif self.is_spark_job():
            return f"jade-internal run-spark-cluster {self.name} {self._model.command}"
        return self._model.command

    @property
    def estimated_run_minutes(self):
        return self._model.estimated_run_minutes

    @property
    def extension(self):
        return _EXTENSION

    @property
    def name(self):
        return self._create_name() if self._model.name is None else self._model.name

    def _create_name(self):
        return str(self._model.job_id)

    def serialize(self):
        assert self._model.job_id is not None
        # If job sizes get huge then we should exclude parameters with default values.
        return self._model.dict()

    @classmethod
    def deserialize(cls, data):
        return cls(**data)

    @property
    def cancel_on_blocking_job_failure(self):
        return self._model.cancel_on_blocking_job_failure

    def get_blocking_jobs(self):
        return self._model.blocked_by

    @property
    def model(self):
        return self._model

    def remove_blocking_job(self, name):
        self._model.blocked_by.remove(name)

    def set_blocking_jobs(self, blocking_jobs):
        self._model.blocked_by = blocking_jobs

    @property
    def submission_group(self):
        return self._model.submission_group

    def is_spark_job(self):
        return self._model.spark_config is not None and self._model.spark_config.enabled


class GenericCommandParametersModel(JadeBaseModel):
    """Model definition for generic command parameters"""

    name: Optional[str] = Field(
        title="name",
        description="If not set Jade will use the job_id converted to a string. Must be unique.",
    )
    use_multi_node_manager: bool = Field(
        title="use_multi_node_manager",
        description="If true JADE will run this job with its multi-node manager.",
        default=False,
    )
    spark_config: Optional[SparkConfigModel] = Field(
        title="spark_config",
        description="If enabled JADE will run this job on a Spark cluster.",
        default=None,
    )
    command: str = Field(
        title="command",
        description="Command that can be invoked in a terminal (shell characters not allowed)",
    )
    blocked_by: Set[str] = Field(
        title="blocked_by",
        description="Array of job names that must complete before this one can start.",
        default=set(),
    )
    cancel_on_blocking_job_failure: bool = Field(
        title="cancel_on_blocking_job_failure",
        description="If true JADE will cancel this job if any of its blocking jobs fail.",
        default=False,
    )
    estimated_run_minutes: Optional[int] = Field(
        title="estimated_run_minutes",
        description="JADE will use this value along with num-parallel-processes-per-node and "
        "walltime to build per-node batches of jobs if time-based-batching is enabled.",
    )
    submission_group: str = Field(
        title="submission_group",
        description="Optional name of a submission group",
        default=DEFAULT_SUBMISSION_GROUP,
    )
    append_job_name: bool = Field(
        title="append_job_name",
        description="If true JADE will append --jade-job-name=X where X is the job's name.",
        default=False,
    )
    append_output_dir: bool = Field(
        title="append_output_dir",
        description="If true JADE will append --jade-runtime-output=X where X is the output "
        "directory specified in jade submit-jobs.",
        default=False,
    )
    ext: Dict = Field(
        title="ext",
        description="User-defined extension data to be used at runtime. Must be serializable in "
        "JSON format.",
        default={},
    )
    job_id: Optional[int] = Field(
        title="job_id",
        description="Unique job identifier, generated by JADE",
    )
    extension: str = Field(
        title="extension",
        description="job extension type, generated by JADE",
        default=_EXTENSION,
    )

    @validator("append_output_dir")
    def handle_append_output_dir(cls, value, values):
        spark_enabled = False
        if values["spark_config"] is not None:
            spark_enabled = getattr(values["spark_config"], "enabled")
        if values["use_multi_node_manager"] or spark_enabled:
            logger.debug(
                "Override 'append_output_dir' because 'use_multi_node_manager' is set or spark is enabled"
            )
            return True
        return value

    @validator("blocked_by")
    def handle_blocked_by(cls, value):
        return {str(x) for x in value}

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        # Keep the config file smaller by skipping values that are defaults.
        for field in (
            "use_multi_node_manager",
            "spark_config",
            "append_job_name",
            "append_output_dir",
            "ext",
        ):
            if data[field] == GenericCommandParametersModel.__fields__[field].default:
                data.pop(field)
        return data
