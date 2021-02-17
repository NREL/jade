"""Models for HPC configurations"""

import enum
from typing import Optional, Union

from pydantic import Field
from pydantic.class_validators import root_validator

from jade.hpc.common import HpcType
from jade.models.base import JadeBaseModel


class SlurmConfig(JadeBaseModel):
    """Defines config options for the SLURM queueing system."""

    account: str = Field(
        title="account",
        description="project account to use",
    )
    partition: Optional[str] = Field(
        title="partition",
        description="HPC partition on which to submit",
        default="short",
    )
    qos: Optional[str] = Field(
        title="qos",
        description="set to high to get faster node allocations at twice the cost",
        default=None,
    )
    walltime: Optional[str] = Field(
        title="walltime",
        description="maximum time allocated to each node",
        default="4:00:00",
    )

    @root_validator(pre=True)
    def handle_allocation(cls, values: dict) -> dict:
        if "allocation" in values:
            values["account"] = values.pop("allocation")
        return values


class FakeHpcConfig(JadeBaseModel):
    """Defines config options for the fake queueing system."""

    walltime: str = Field(
        title="walltime",
        description="maximum time allocated to each node",
    )


class LocalHpcConfig(JadeBaseModel):
    """Defines config options when there is no HPC."""


class HpcConfig(JadeBaseModel):
    """Defines config options for the HPC."""

    hpc_type: HpcType = Field(
        title="hpc_type",
        description="type of HPC queueing system (such as 'slurm')",
    )
    job_prefix: Optional[str] = Field(
        title="job_prefix",
        description="prefix added to each HPC job name",
        default="job",
    )
    hpc: Union[SlurmConfig, FakeHpcConfig, LocalHpcConfig] = Field(
        title="hpc",
        description="interface-specific config options",
    )
