"""HPC management implementation functionality"""

import abc
import getpass
import os

from jade.exceptions import InvalidParameter
from jade.utils.utils import load_data


class HpcManagerInterface(abc.ABC):
    """Defines the implementation interface for managing an HPC."""

    USER = getpass.getuser()

    @abc.abstractmethod
    def am_i_manager(self):
        """Return True if the current node is the manager node.

        Returns
        -------
        bool

        """

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
    def check_statuses(self):
        """Check the statuses of all user jobs.

        Returns
        -------
        dict
            key is job_id, value is HpcJobStatus

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
    def get_node_id(self):
        """Return the node ID of the current system.

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
    def list_active_nodes(self, job_id):
        """Return the nodes currently participating in the job.

        Parameters
        ----------
        job_id : str

        Returns
        -------
        list
            list of node hostnames

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
