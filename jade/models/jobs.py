"""Models for jobs"""

import enum
from typing import List, Optional, Set, Union

from pydantic import Field

from jade.models.base import JadeBaseModel


class JobState(enum.Enum):
    """Job states"""

    NOT_SUBMITTED = "not_submitted"
    SUBMITTED = "submitted"
    DONE = "done"


class Job(JadeBaseModel):
    """Describes the status of all jobs in the JADE config."""

    name: str = Field(
        title="name",
        description="name of the job, must be unique",
    )
    blocked_by: Set[str] = Field(
        title="blocked_by",
        description="job names that are blocking this job",
    )
    cancel_on_blocking_job_failure: Optional[bool] = Field(
        title="cancel_on_blocking_job_failure",
        description="cancel job if one of its blocking jobs fails",
        default=False,
    )
    state: JobState = Field(
        title="state",
        description="job state",
    )


class JobStatus(JadeBaseModel):
    """Describes all jobs in the JADE config."""

    jobs: List[Job] = Field(
        title="jobs",
        description="describes all jobs in the JADE config",
    )
    hpc_job_ids: List[str] = Field(
        title="hpc_job_ids",
        description="HPC job IDs for active jobs",
    )
    batch_index: Optional[int] = Field(
        title="batch_index",
        description="HPC batch index",
        default=1,
    )
    version: int = Field(
        title="version",
        description="version of the statuses, increments with each update",
    )
