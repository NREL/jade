"""Defines JADE base model"""

from pydantic import BaseModel


class JadeBaseModel(BaseModel):
    """Base class for JADE models."""

    class Config:
        title = "JadeBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"
        use_enum_values = False
