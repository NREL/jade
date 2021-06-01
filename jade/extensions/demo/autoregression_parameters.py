"""
Implement the JobParametersInterface for auto-regression analysis.
"""

from collections import namedtuple

from jade.common import DEFAULT_SUBMISSION_GROUP
from jade.jobs.job_parameters_interface import JobParametersInterface


class AutoRegressionParameters(JobParametersInterface):
    """
    A class used for creating auto-regression job.
    """

    parameters_type = namedtuple("AutoRegression", "country")
    _EXTENSION = "demo"

    def __init__(self, country, data):
        """
        Init auto-regression parameter class

        Parameters
        ----------
        country: str
            The name of a country.
        data: str
            The path to the csv file containing the GDP data.
        """
        self.country = country
        self.data = data
        self._name = self._create_name()

    def __str__(self):
        return "<AutoRegressionParameters: {}>".format(self.name)

    @property
    def extension(self):
        return self._EXTENSION

    @property
    def name(self):
        return self._name

    def _create_name(self):
        return self.country.replace(" ", "_").lower()

    def serialize(self):
        return {
            "country": self._name,
            "data": self.data,
            "extension": self.extension,
        }

    @classmethod
    def deserialize(cls, param):
        return cls(param["country"], param["data"])

    @property
    def estimated_run_minutes(self):
        return None

    @property
    def cancel_on_blocking_job_failure(self):
        return False

    def get_blocking_jobs(self):
        return set()

    def remove_blocking_job(self, name):
        pass

    def set_blocking_jobs(self, blocking_jobs):
        pass

    @property
    def submission_group(self):
        return DEFAULT_SUBMISSION_GROUP

    @submission_group.setter
    def submission_group(self, group):
        return
