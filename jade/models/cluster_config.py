"""Defines the cluster configuration"""

from typing import Optional

from pydantic import Field

from jade.models import JadeBaseModel, SubmitterParams


class ClusterConfig(JadeBaseModel):
    """Describes the roles of compute nodes participating in a JADE config."""

    submitter: Optional[str] = Field(
        title="submitter",
        description="defines the current submitter, hostname or None",
        default=None,
    )
    submitter_params: SubmitterParams = Field(
        title="submitter_params",
        description="defines the submitter options selected by the user",
    )
    path: str = Field(
        title="path",
        description="directory on shared filesystem containing config",
    )
    pipeline_stage_num: Optional[int] = Field(
        title="pipeline_stage_num",
        description="stage number if the config is part of a pipeline",
    )
    num_jobs: int = Field(
        title="num_jobs",
        description="total number of jobs in configuration",
    )
    submitted_jobs: Optional[int] = Field(
        title="submitted_jobs",
        description="number of jobs submitted (never decrements)",
        default=0,
    )
    completed_jobs: Optional[int] = Field(
        title="completed_jobs",
        description="number of jobs that have completed",
        default=0,
    )
    is_complete: Optional[bool] = Field(
        title="is_complete",
        description="set to True when the submission is complete",
        default=False,
    )
    version: int = Field(
        title="version",
        description="version of the config, increments with each update",
    )
