import os
import shutil

import pytest

from jade.exceptions import InvalidConfiguration
from jade.extensions.generic_command import GenericCommandInputs
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.models.hpc import HpcConfig
from jade.models.submitter_params import SubmitterParams
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import load_data
from jade.test_common import FAKE_HPC_CONFIG


TEST_FILENAME = "inputs.txt"
CONFIG_FILE = "test-config.json"
OUTPUT = "test-output"
SUBMIT_JOBS = "jade submit-jobs"


@pytest.fixture
def job_fixture():
    yield
    for path in (TEST_FILENAME, CONFIG_FILE, OUTPUT):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_job_configuration__check_job_dependencies_blocking(job_fixture):
    with open(TEST_FILENAME, "w") as f_out:
        f_out.write("echo hello world\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(job_inputs=inputs)
    for job_param in inputs.iter_jobs():
        config.add_job(job_param)
    assert config.get_num_jobs() == 1

    hpc_config = HpcConfig(**load_data(FAKE_HPC_CONFIG))
    params = SubmitterParams(hpc_config=hpc_config)
    job = config.get_job("1")
    job.blocked_by.add("10")
    with pytest.raises(InvalidConfiguration):
        config.check_job_dependencies(params)

    # While we have this setup, verify that submit-jobs calls this function.
    config.dump(CONFIG_FILE)
    cmd = f"{SUBMIT_JOBS} {CONFIG_FILE} --output={OUTPUT} " "--poll-interval=.1 "
    ret = run_command(cmd)
    assert ret != 0


def test_job_configuration__check_job_dependencies_estimate(job_fixture):
    with open(TEST_FILENAME, "w") as f_out:
        f_out.write("echo hello world\n")

    inputs = GenericCommandInputs(TEST_FILENAME)
    config = GenericCommandConfiguration(job_inputs=inputs)
    for job_param in inputs.iter_jobs():
        config.add_job(job_param)
    assert config.get_num_jobs() == 1

    hpc_config = HpcConfig(**load_data(FAKE_HPC_CONFIG))
    params = SubmitterParams(hpc_config=hpc_config, per_node_batch_size=0)
    with pytest.raises(InvalidConfiguration):
        config.check_job_dependencies(params)
