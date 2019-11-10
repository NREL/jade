"""Common definitions for HPC functionality"""

from collections import namedtuple
import enum


class HpcJobStatus(enum.Enum):
    """Represents the status of an HPC job."""
    UNKNOWN = "unknown"
    NONE = "none"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"


HpcJobInfo = namedtuple("HpcJobStatusInfo", "job_id, name, status")


class HpcQos(enum.Enum):
    """HPC Quality of Service values"""
    LOW = 0
    HIGH = 1


class HpcType(enum.Enum):
    """HPC types"""
    LOCAL = "Local"
    PBS = "PBS"
    SLURM = "SLURM"
    FAKE = "Fake"
