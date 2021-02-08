"""Tests HpcManager functionality."""

import copy
import os

from pydantic import ValidationError
import pytest

from jade.common import OUTPUT_DIR
from jade.hpc.common import HpcType, HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.hpc.slurm_manager import SlurmManager
from jade.jobs.job_submitter import DEFAULTS
from jade.exceptions import InvalidParameter
from jade.models import HpcConfig
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import dump_data, load_data


HPC_CONFIG = load_data(DEFAULTS["hpc_config_file"])


def hpc_config(hpc_type, **kwargs):
    config = {
        "hpc_type": hpc_type,
        "hpc": {
            "account": "abc",
            "partition": "short",
            "walltime": "4:00:00",
        },
    }
    for key, val in kwargs.items():
        config["hpc"][key] = val

    return HpcConfig(**config)


def test_create_slurm():
    create_hpc_manager("slurm")
    config = hpc_config("slurm")
    bad_config = config.dict()
    bad_config["hpc"].pop("account")
    with pytest.raises(ValidationError):
        HpcConfig(**bad_config)


def test_create_fake():
    create_hpc_manager("fake")


def test_create_local():
    create_hpc_manager("local")


def test_slurm_check_statuses():
    with open("tests/data/squeue_status.txt") as f_in:
        text = f_in.read()

    sts = SlurmManager._get_statuses_from_output(text)
    assert list(sts.keys()) == ["10", "11", "12", "13"]
    assert list(sts.values()) == [
        HpcJobStatus.QUEUED,
        HpcJobStatus.QUEUED,
        HpcJobStatus.RUNNING,
        HpcJobStatus.RUNNING,
    ]


def test_create_submission_script():
    mgr = create_hpc_manager("slurm")
    script = "run.sh"
    required = ["account", "time", "job-name", "output",
                "error", "#SBATCH"]
    required += [script]
    try:
        submission_script = "submit.sh"
        mgr._intf.create_submission_script("test", script,
                                           submission_script, ".")
        assert os.path.exists(submission_script)
        with open(submission_script) as fp_in:
            data = fp_in.read()
            for term in required:
                assert term in data
    finally:
        os.remove(submission_script)


def test_qos_setting():
    mgr = create_hpc_manager("slurm", qos="high")
    text = mgr._intf._create_submission_script_text("name", "run.sh", ".")
    found = False
    for line in text:
        if "qos" in line:
            found = True
    assert found

    # With qos not set.
    mgr = create_hpc_manager("slurm")
    text = mgr._intf._create_submission_script_text("name", "run.sh", ".")
    found = False
    for line in text:
        if "qos" in line:
            found = True
    assert not found


def test_get_stripe_count():
    output = "stripe_count:  16 stripe_size:   1048576 stripe_offset: -1"
    assert SlurmManager._get_stripe_count(output) == 16


def create_hpc_manager(hpc_type, **kwargs):
    mgr = None
    config = hpc_config(hpc_type, **kwargs)
    mgr = HpcManager(config, OUTPUT_DIR)

    if hpc_type == "slurm":
        assert mgr.hpc_type == HpcType.SLURM
    elif hpc_type == "fake":
        assert mgr.hpc_type == HpcType.FAKE
    elif hpc_type == "local":
        assert mgr.hpc_type == HpcType.LOCAL
    else:
        assert False, "unknown hpc_type={}".format(hpc_type)

    return mgr
