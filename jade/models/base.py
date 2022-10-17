"""Defines JADE base model"""

from pathlib import Path

from pydantic import BaseModel

from jade.utils.utils import load_data


class JadeBaseModel(BaseModel):
    """Base class for JADE models."""

    class Config:
        title = "JadeBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"
        use_enum_values = False
        allow_population_by_field_name = True

    @classmethod
    def load(cls, path: Path):
        """Load a model from a file."""
        return cls(**load_data(path))
