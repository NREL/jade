"""Defines parameters for submitting jobs to an HPC."""

from typing import Optional

from pydantic import Field

from jade.models import JadeBaseModel, SubmitterParams


class SubmissionGroup(JadeBaseModel):
    """Defines parameters for submitting jobs to an HPC."""

    name: str = Field(
        title="name",
        description="User-defined name of the group",
    )
    submitter_params: SubmitterParams = Field(
        title="submitter_params",
        description="Submission parameters for the group",
    )


def make_submission_group_lookup(submission_groups):
    """Return the submission groups in a dict keyed by name.

    Returns
    -------
    dict

    """
    return {x.name: x for x in submission_groups}
