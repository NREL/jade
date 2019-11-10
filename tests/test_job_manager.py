import copy
import os

from jade.jobs.job_runner import JobRunner
from jade.jobs.job_submitter import JobSubmitter, DEFAULTS
from jade.extensions.pydss_simulation.pydss_configuration import PyDssConfiguration
from jade.extensions.pydss_simulation.pydss_inputs import PyDssInputs
from jade.extensions.pydss_simulation.pydss_simulation import PyDssSimulation


CONFIG_FILE = "test-config.json"
HPC_CONFIG_FILE = DEFAULTS["hpc_config_file"]


def get_config():
    inputs = PyDssInputs("model-inputs")
    config = PyDssConfiguration.auto_config(inputs)
    return config


# TODO: automatically delete output directory at the end tof test execution.


class TestJobSubmitter(object):

    def test_job_submitter(self):
        config = get_config()
        config.dump(CONFIG_FILE)

        try:
            mgr = JobSubmitter(CONFIG_FILE)
            assert not mgr.get_completed_results()
            assert config.get_num_jobs() == mgr.get_num_jobs()
        finally:
            os.remove(CONFIG_FILE)

class TestJobRunner(object):

    def test_job_runner(self):
        config = get_config()
        config.dump(CONFIG_FILE)

        try:
            mgr = JobRunner(CONFIG_FILE)
            assert not mgr.get_completed_results()
            assert config.get_num_jobs() == mgr.get_num_jobs()
        finally:
            os.remove(CONFIG_FILE)
