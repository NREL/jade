
import os
import shutil

import pytest

import jade
from jade.jobs.cluster import Cluster
from jade.common import CONFIG_FILE
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.models import SubmitterOptions, HpcConfig, SlurmConfig


OUTPUT = "test-output"

@pytest.fixture
def jade_output():
    os.makedirs(OUTPUT, exist_ok=True)
    commands = ["echo 'hello'"] * 2
    cmd_file = os.path.join(OUTPUT, "commands.txt")
    with open(cmd_file, "w") as f_out:
        for cmd in commands:
            f_out.write(cmd + "\n")

    config = GenericCommandConfiguration.auto_config(cmd_file)
    config_file = os.path.join(OUTPUT, CONFIG_FILE)
    config.dump(config_file)

    yield config, OUTPUT

    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)


def test_cluster__create(jade_output):
    jade_config, output = jade_output
    hpc_config = HpcConfig(hpc_type="slurm", hpc=SlurmConfig(account="abc"))
    options = SubmitterOptions(hpc_config=hpc_config)
    cluster = Cluster.create(output, options, jade_config)
    assert cluster.config.num_jobs == 2
    assert cluster.config.submitted_jobs == 0
    assert cluster.config.completed_jobs == 0
    assert not cluster.all_jobs_submitted()
    assert cluster.has_submitter()
    assert cluster.am_i_submitter()
    cluster.demote_from_submitter()
    assert not cluster.am_i_submitter()

    cluster2, _ = Cluster.deserialize(output)
    assert not cluster.has_submitter()
    assert not cluster.am_i_submitter()
    assert cluster.config.num_jobs == 2
    assert cluster.config.submitted_jobs == 0
    assert cluster.config.completed_jobs == 0
