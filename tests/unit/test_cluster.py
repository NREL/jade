import os
import shutil

import pytest

from jade.jobs.cluster import Cluster, ConfigVersionMismatch
from jade.common import CONFIG_FILE
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.models import SubmitterParams, HpcConfig, SlurmConfig


OUTPUT = "test-output"


@pytest.fixture
def cluster():
    os.makedirs(OUTPUT, exist_ok=True)
    commands = ["echo 'hello'"] * 2
    cmd_file = os.path.join(OUTPUT, "commands.txt")
    with open(cmd_file, "w") as f_out:
        for cmd in commands:
            f_out.write(cmd + "\n")

    jade_config = GenericCommandConfiguration.auto_config(cmd_file)
    config_file = os.path.join(OUTPUT, CONFIG_FILE)
    jade_config.dump(config_file)
    hpc_config = HpcConfig(hpc_type="slurm", hpc=SlurmConfig(account="abc"))
    cluster = Cluster.create(OUTPUT, jade_config)

    yield cluster

    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)


def test_cluster__create(cluster):
    assert cluster.config.num_jobs == 2
    assert cluster.config.submitted_jobs == 0
    assert cluster.config.completed_jobs == 0
    assert not cluster.all_jobs_submitted()
    assert cluster.has_submitter()
    assert cluster.am_i_submitter()
    assert cluster.config.version == 1
    assert cluster.job_status.version == 1
    cluster.demote_from_submitter()
    assert not cluster.am_i_submitter()
    assert cluster.config.version == 2

    cluster, _ = Cluster.deserialize(cluster.config.path)
    assert not cluster.has_submitter()
    assert not cluster.am_i_submitter()
    assert cluster.job_status is None
    assert cluster.config.num_jobs == 2
    assert cluster.config.submitted_jobs == 0
    assert cluster.config.completed_jobs == 0
    assert cluster.config.version == 2

    cluster, promoted = Cluster.deserialize(
        cluster.config.path,
        try_promote_to_submitter=True,
        deserialize_jobs=True,
    )
    assert promoted
    assert cluster.am_i_submitter()
    assert cluster.job_status is not None
    assert cluster.config.version == 3


def test_cluster__submit_jobs(cluster):
    submitted_jobs = cluster.job_status.jobs
    cluster.update_job_status(submitted_jobs, [], set(), [], [1], 1)
    assert cluster.config.submitted_jobs == 2
    assert cluster.job_status.version == 2


def test_cluster__version_mismatch(cluster):
    cluster.demote_from_submitter()
    assert not cluster.am_i_submitter()
    with open(cluster._config_version_file, "w") as f_out:
        f_out.write(str(cluster.config.version + 1) + "\n")
    with open(cluster._job_status_version_file, "w") as f_out:
        f_out.write(str(cluster.job_status.version + 1) + "\n")

    try:
        with pytest.raises(ConfigVersionMismatch):
            cluster.promote_to_submitter()
    finally:
        os.remove(Cluster.get_lock_file(cluster.config.path))

    try:
        with pytest.raises(ConfigVersionMismatch):
            submitted_jobs = cluster.job_status.jobs
            cluster.update_job_status(submitted_jobs, [], set(), [], [1], 1)
    finally:
        os.remove(Cluster.get_lock_file(cluster.config.path))
