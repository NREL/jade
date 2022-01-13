"""Models for Singularity containers"""

from pathlib import Path
from typing import Optional

from pydantic import Field, validator

from jade.models import JadeBaseModel


SINGULARITY_SETUP_COMMANDS = """module load singularity-container
export LD_LIBRARY_PATH=/usr/lib64:/nopt/slurm/current/lib64/slurm:$LD_LIBRARY_PATH
export PATH=$PATH:/nopt/slurm/current/bin
"""

BINDS = "/nopt,/usr/lib64/libreadline.so.6,/usr/lib64/libhistory.so.6,/usr/lib64/libtinfo.so.5,/var/run/munge,/usr/lib64/libmunge.so.2,/usr/lib64/libmunge.so.2.0.0,/run/munge"
SINGULARITY_RUN_COMMAND = f"singularity run -B {BINDS} -B /scratch:/scratch -B /projects:/projects"


class SingularityParams(JadeBaseModel):
    """Defines parameters for using Singularity containers"""

    # This must be first for validation to work.
    enabled: bool = Field(
        title="enabled",
        description="Run all jobs through a Singularity container",
        default=False,
    )
    container: Optional[str] = Field(
        title="container",
        description="Path to Singularity container",
    )
    load_command: str = Field(
        title="load_command",
        description="Command to load the singularity environment. Can be empty.",
        default="module load singularity-container",
    )
    run_command: str = Field(
        title="run_command",
        description="Command to run the Singularity container",
        default=SINGULARITY_RUN_COMMAND,
    )
    setup_commands: str = Field(
        title="setup_commands",
        description="Commands to execute within the Singularity container",
        default=SINGULARITY_SETUP_COMMANDS,
    )

    @validator("container")
    def check_container(cls, container, values):
        if values["enabled"]:
            if container is None:
                raise ValueError("'container' must be set")
            if not Path(container).exists():
                raise ValueError(f"{container} does not exist")
        return container
