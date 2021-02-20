"""Models for pipeline execution"""

from typing import List, Optional

from pydantic import Field

from jade.models import JadeBaseModel, SubmitterParams


class PipelineStage(JadeBaseModel):
    """Describes one stage of a pipeline."""

    auto_config_cmd: str = Field(
        title="auto_config_cmd",
        description="command used to create the JADE configuration",
    )
    config_file: str = Field(
        title="config_file",
        description="JADE configuration file",
    )
    stage_num: int = Field(
        title="stage_num",
        description="1-based ID of the stage in the pipeline",
    )
    path: Optional[str] = Field(
        title="path",
        description="directory on shared filesystem containing config",
    )
    return_code: Optional[int] = Field(
        title="return_code",
        description="return code of stage; 0 is success",
    )
    submitter_params: SubmitterParams = Field(
        title="submitter_params",
        description="defines the submitter params selected by the user",
    )


class PipelineConfig(JadeBaseModel):
    """Describes the configuration and status of a pipeline."""

    path: Optional[str] = Field(
        title="path",
        description="directory on shared filesystem containing config",
    )
    stage_num: int = Field(
        title="stage_num",
        description="number of current stage",
    )
    stages: List[PipelineStage] = Field(
        title="stages",
        description="stages in the pipeline",
    )
    is_complete: Optional[bool] = Field(
        title="is_complete",
        description="set to True when the pipeline is complete",
        default=False,
    )
