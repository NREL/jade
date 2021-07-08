from pydantic import BaseModel

from jade.models.base import JadeBaseModel
from jade.models.hpc import HpcConfig, SlurmConfig, FakeHpcConfig, LocalHpcConfig
from jade.models.submitter_params import SubmitterParams
from jade.models.submission_group import SubmissionGroup
from jade.models.jobs import Job, JobState, JobStatus
from jade.models.cluster_config import ClusterConfig
from jade.models.pipeline import PipelineConfig, PipelineStage


def get_model_defaults(model_class: BaseModel):
    """Return the default values for fields in a Pydantic BaseModel.
    If a field doesn't have a default then return None.
    Default values may also be None.

    Returns
    -------
    dict

    """
    return {x: y.get("default") for x, y in model_class.schema()["properties"].items()}
