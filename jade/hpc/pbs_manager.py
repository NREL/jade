"""Manages PBS jobs."""

import logging

from jade.enums import Status
from jade.hpc.common import HpcJobStatus
from jade.hpc.hpc_manager_interface import HpcManagerInterface
from jade.utils.subprocess_manager import run_command
from jade.utils import utils


logger = logging.getLogger(__name__)

PBS_SCRIPT = """#!/bin/bash
#PBS -N {name} # job name
#PBS -A {alloc} # allocation account
#PBS -l {qos}
#PBS -q {queue} # queue (debug, short, batch, or long)
#PBS -o {path}/{name}_$PBS_JOBID.o
#PBS -e {path}/{name}_$PBS_JOBID.e
{qos_str}
{feature}
echo Running on: $HOSTNAME, Machine Type: $MACHTYPE
{script}

wait
"""


class PbsManager(HpcManagerInterface):
    """Manages PBS jobs."""

    _STATUSES = {
        "Q": HpcJobStatus.QUEUED,
        "R": HpcJobStatus.RUNNING,
        "C": HpcJobStatus.COMPLETE,
    }

    def __init__(self, config):
        self._config = config

    def am_i_manager(self):
        assert False

    def cancel_job(self, job_id):
        return 0

    def check_status(self, name=None, job_id=None):
        qstat_rows = self._qstat()
        if qstat_rows is None:
            return HpcJobStatus.NONE

        # TODO job_id

        return self._get_status_from_output(qstat_rows, name)

    def check_statuses(self):
        assert False

    def check_storage_configuration(self):
        pass

    def create_cluster(self):
        pass

    def create_local_cluster(self):
        pass

    def get_config(self):
        return self._config

    def get_local_scratch(self):
        return "."

    @staticmethod
    def get_num_cpus():
        return 18

    @staticmethod
    def _get_status_from_output(qstat_rows, name):
        # column location of various job identifiers
        col_loc = {"id": 0, "name": 3}

        # reverse the list so most recent jobs are first
        qstat_rows.reverse()

        # update job status from qstat list
        status = HpcJobStatus.UNKNOWN
        for row in qstat_rows:
            row = row.split()
            # make sure the row is long enough to be a job status listing
            # TODO regex?
            if len(row) > 10:
                if row[col_loc["name"]].strip() == name.strip():
                    # Job status is located at the -2 index
                    status = PbsManager._STATUSES.get(row[-2], HpcJobStatus.UNKNOWN)
                    if status is HpcJobStatus.UNKNOWN:
                        logger.error("Unknown PBS job status: %s", row[-2])
                    break
        return status

    def _qstat(self):
        """Run the PBS qstat command and return the stdout split to rows.

        Returns
        -------
        qstat_rows : list | None
            List of strings where each string is a row in the qstat printout.
            Returns None if qstat is empty.

        """
        cmd = "qstat -u {user}".format(user=self.USER)
        output = {}
        run_command(cmd, output)
        if not output["stdout"]:
            # No jobs are currently running.
            return None

        qstat_rows = output["stdout"].split("\n")
        return qstat_rows

    def create_submission_script(self, name, script, filename, path="."):
        feature = self._config.get("feature")
        if feature is None:
            feature = ""
        else:
            feature = "#PBS -l feature={}".format("feature")

        script = PBS_SCRIPT.format(
            name=name,
            alloc=self._config["allocation"],
            qos=self._config["qos"],
            queue=self._config["queue"],
            path=path,
            feature=feature,
            script=script,
        )

        utils.create_script(filename, script)

    def list_active_nodes(self, job_id):
        assert False

    def log_environment_variables(self):
        pass

    def submit(self, filename):
        output = {}
        ret = run_command("qsub {}".format(filename), output)
        if ret == 0:
            result = Status.GOOD
            job_id = output["stdout"]
        else:
            result = Status.ERROR
            job_id = None

        return result, job_id, output["stderr"]
