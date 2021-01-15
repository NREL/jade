"""Tests HpcManager functionality."""

import copy
import os

import pytest

from jade.common import OUTPUT_DIR
from jade.hpc.common import HpcType, HpcJobStatus
from jade.hpc.hpc_manager import HpcManager
from jade.hpc.slurm_manager import SlurmManager
from jade.jobs.job_submitter import DEFAULTS
from jade.exceptions import InvalidParameter
from jade.utils.utils import dump_data, load_data


HPC_CONFIG = load_data(DEFAULTS["hpc_config_file"])


@pytest.fixture
def hpc_fixture():
    original = os.environ.get("NREL_CLUSTER")
    yield

    if original is None:
        if "NREL_CLUSTER" in os.environ:
            os.environ.pop("NREL_CLUSTER")
    else:
        os.environ["NREL_CLUSTER"] = original


def hpc_config():
    return copy.deepcopy(HPC_CONFIG)


def test_create_slurm(hpc_fixture):
    create_hpc_manager("eagle", hpc_config())

    bad_config = hpc_config()
    bad_config["hpc"].pop("allocation")
    with pytest.raises(InvalidParameter):
        create_hpc_manager("eagle", bad_config)

    optional_config = copy.deepcopy(hpc_config())
    create_hpc_manager("eagle", optional_config)


def test_create_slurm_invalid_file(hpc_fixture):
    os.environ["NREL_CLUSTER"] = "eagle"
    with pytest.raises(FileNotFoundError):
        HpcManager("invalid_filename", OUTPUT_DIR)


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


def test_create_pbs(hpc_fixture):
    create_hpc_manager("peregrine", hpc_config())


def test_create_submission_script(hpc_fixture):
    config = hpc_config()
    mgr = create_hpc_manager("eagle", config)
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


def test_qos_setting(hpc_fixture):
    config = hpc_config()

    # With qos set.
    config["hpc"]["qos"] = "high"
    mgr = create_hpc_manager("eagle", config)
    text = mgr._intf._create_submission_script_text("name", "run.sh", ".")
    found = False
    for line in text:
        if "qos" in line:
            found = True
    assert found

    # With qos not set.
    config = hpc_config()
    if "hpc" in config["hpc"]:
        config["hpc"].pop("qos")
    mgr = create_hpc_manager("eagle", config)
    text = mgr._intf._create_submission_script_text("name", "run.sh", ".")
    found = False
    for line in text:
        if "qos" in line:
            found = True
    assert not found


def test_get_stripe_count(hpc_fixture):
    output = "stripe_count:  16 stripe_size:   1048576 stripe_offset: -1"
    assert SlurmManager._get_stripe_count(output) == 16


def create_hpc_manager(cluster, config):
    os.environ["NREL_CLUSTER"] = cluster
    mgr = None
    try:
        hpc_file = "test-hpc-config.toml"
        dump_data(config, hpc_file)

        mgr = HpcManager(hpc_file, OUTPUT_DIR)
    finally:
        os.remove(hpc_file)

    if cluster == "eagle":
        assert mgr.hpc_type == HpcType.SLURM
    elif cluster == "peregrine":
        assert mgr.hpc_type == HpcType.PBS
    else:
        assert False, "unknown cluster={}".format(cluster)

    return mgr
