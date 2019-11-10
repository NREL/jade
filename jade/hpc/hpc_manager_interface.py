"""HPC management implementation functionality"""

import abc
import getpass
import os

from jade.exceptions import InvalidParameter
from jade.utils.utils import load_data


class HpcManagerInterface(abc.ABC):
    """Defines the implementation interface for managing an HPC."""

    USER = getpass.getuser()

    def create_config(self, config_file):
        """Creates a configuration from a config file.

        Parameters
        ----------
        config_file : str | dict
            HPC config

        Returns
        -------
        dict

        """
        if isinstance(config_file, dict):
            config = config_file
        else:
            if not os.path.exists(config_file):
                raise FileNotFoundError(
                    f"HPC config file {config_file} does not exist"
                )
            config = load_data(config_file)

        for param in self.get_required_config_params():
            if param not in config["hpc"]:
                raise InvalidParameter(f"missing HPC config parameter {param}")

        for param, val in self.get_optional_config_params().items():
            if param not in config["hpc"]:
                config["hpc"][param] = val

        return config

    @abc.abstractmethod
    def cancel_job(self, job_id):
        """Cancel job.

        Parameters
        ----------
        job_id : str

        Returns
        -------
        int
            return code

        """

    @abc.abstractmethod
    def check_status(self, name=None, job_id=None):
        """Check the status of a job. Either name or job_id must be passed.

        Parameters
        ----------
        name : str
            job name
        job_id : str
            job ID

        Returns
        -------
        HpcJobInfo

        """

    @abc.abstractmethod
    def check_storage_configuration(self):
        """Checks if the storage configuration is appropriate for execution.

        Raises
        ------
        InvalidConfiguration
            Raised if the configuration is not valid

        """

    @abc.abstractmethod
    def create_submission_script(self, name, script, filename, path):
        """Create the script to queue the jobs to the HPC.

        Parameters
        ----------
        name : str
            job name
        script : str
            script to execute on HPC
        filename : str
            submission script filename
        path : str
            path for stdout and stderr files

        """

    @abc.abstractmethod
    def create_cluster(self):
        """Create a Dask cluster.

        Returns
        -------
        Dask cluster
            SLURM: SLURMCluster

        """

    @abc.abstractmethod
    def create_local_cluster(self):
        """Create a Dask local cluster.

        Returns
        -------
        dask.distributed.LocalCluster

        """

    @abc.abstractmethod
    def get_config(self):
        """Get HPC configuration parameters.

        Returns
        -------
        dict

        """

    @abc.abstractmethod
    def get_local_scratch(self):
        """Get path to local storage space.

        Returns
        -------
        str

        """

    @abc.abstractmethod
    def get_num_cpus(self):
        """Return the number of CPUs in the system.

        Returns
        -------
        int

        """

    @abc.abstractmethod
    def get_optional_config_params(self):
        """Get optional config parameters and default values for the HPC.

        Returns
        -------
        dict
            keys are parameter names, values are default values

        """

    @abc.abstractmethod
    def get_required_config_params(self):
        """Get required config parameters for the HPC.

        Returns
        -------
        tuple
            parameter names

        """

    @abc.abstractmethod
    def log_environment_variables(self):
        """Logs all relevant HPC environment variables."""

    @abc.abstractmethod
    def submit(self, filename):
        """Submit the work to the HPC queue.

        Parameters
        ----------
        filename : str
            HPC script filename

        Returns
        -------
        tuple of Status, str, str
            (Status, job_id, stderr)

        """
