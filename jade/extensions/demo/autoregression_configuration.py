"""
Implement JobConfiguration for auto-regression analysis.
"""
from jade.jobs.job_container_by_key import JobContainerByKey
from jade.jobs.job_configuration import JobConfiguration
from jade.extensions.demo.autoregression_parameters import AutoRegressionParameters


class AutoRegressionConfiguration(JobConfiguration):
    """
    A class used to configure auto-regression jobs
    """

    def _serialize(self, data):
        pass

    def create_from_result(self, job, output_dir):
        return None
