"""Models for submitter options"""

from typing import List, Optional, Set, Union

from pydantic import Field

from jade.enums import ResourceMonitorType
from jade.models import JadeBaseModel, HpcConfig


SINGULARITY_SETUP_COMMANDS = """export LD_LIBRARY_PATH=/usr/lib64:/nopt/slurm/current/lib64/slurm:$LD_LIBRARY_PATH
echo "slurm:x:989:989:SLURM workload manager:/var/lib/slurm:/bin/bash" >> /etc/passwd
echo "slurm:x:989:" >> /etc/group
export PATH=$PATH:/nopt/slurm/current/bin
cd /data
"""

BINDS = "/nopt,/usr/lib64/libreadline.so.6,/usr/lib64/libhistory.so.6,/usr/lib64/libtinfo.so.5,/var/run/munge,/usr/lib64/libmunge.so.2,/usr/lib64/libmunge.so.2.0.0,/run/munge"
SINGULARITY_RUN_COMMAND = f"singularity run -B {BINDS} -B .:/data"


class SingularityParams(JadeBaseModel):
    """Defines parameters for using Singularity containers"""

    container: Optional[str] = Field(
        title="container",
        description="path to Singularity container",
        default="",
    )
    setup_commands: Optional[str] = Field(
        title="setup_commands",
        description="commands to execute within the Singularity container",
        default=SINGULARITY_SETUP_COMMANDS,
    )
    run_command: Optional[str] = Field(
        title="run_command",
        description="command to run the Singularity container",
        default=SINGULARITY_RUN_COMMAND,
    )
    enabled: Optional[bool] = Field(
        title="enabled",
        description="Run all jobs through a Singularity container",
        default=False,
    )


class SubmitterParams(JadeBaseModel):
    """Defines the submitter options selected by the user."""

    generate_reports: Optional[bool] = Field(
        title="generate_reports",
        description="Controls whether to generate reports after completion",
        default=True,
    )
    hpc_config: HpcConfig = Field(
        title="hpc_config",
        description="HPC config options",
    )
    max_nodes: Optional[int] = Field(
        title="max_nodes",
        description="Max number of compute nodes to use simultaneously, default is unbounded",
        default=None,
    )
    num_processes: Optional[int] = Field(
        title="num_processes",
        description="Number of processes to run in parallel on each node",
        default=None,
    )
    per_node_batch_size: Optional[int] = Field(
        title="per_node_batch_size",
        description="How many jobs to assign to each node",
        default=500,
    )
    node_setup_script: Optional[str] = Field(
        title="node_setup_script",
        description="Script to run on each node before starting jobs",
        default=None,
    )
    node_shutdown_script: Optional[str] = Field(
        title="node_shutdown_script",
        description="Script to run on each node after completing jobs",
        default=None,
    )
    poll_interval: Optional[int] = Field(
        title="poll_interval",
        description="Interval in seconds on which to poll jobs for status",
        default=60,
    )
    resource_monitor_interval: Optional[int] = Field(
        title="resource_monitor_interval",
        description="Interval in seconds on which to collect resource stats. If None, aggregate"
        "summaries of stats.",
        default=1,
    )
    resource_monitor_type: Optional[ResourceMonitorType] = Field(
        title="resource_monitor_type",
        description=f"Type of resource monitoring to perform. Options: {list(ResourceMonitorType)}",
        default=ResourceMonitorType.AGGREGATION,
    )
    try_add_blocked_jobs: Optional[bool] = Field(
        title="try_add_blocked_jobs",
        description="Add blocked jobs to a batch if all blocking jobs are in the batch. "
        "Be aware of time constraints.",
        default=True,
    )
    time_based_batching: Optional[bool] = Field(
        title="time_based_batching",
        description="Use time-based batching instead of job-count-based batching",
        default=False,
    )
    dry_run: Optional[bool] = Field(
        title="dry_run",
        description="Dry run mode; don't start any jobs",
        default=False,
    )
    verbose: Optional[bool] = Field(
        title="verbose",
        description="Enable debug logging",
        default=False,
    )
    singularity_params: Optional[SingularityParams] = Field(
        title="singularity_params",
        description="Singularity container parameters",
        default=SingularityParams(),
    )
