"""SLURM management functionality"""

import logging
import os
import re

from jade.enums import Status
from jade.exceptions import ExecutionError  # , InvalidConfiguration
from jade.hpc.common import HpcJobStatus, HpcJobInfo
from jade.hpc.hpc_manager_interface import HpcManagerInterface
from jade.utils.run_command import check_run_command, run_command
from jade.utils import utils


logger = logging.getLogger(__name__)


class SlurmManager(HpcManagerInterface):
    """Manages Slurm jobs."""

    _STATUSES = {
        "PENDING": HpcJobStatus.QUEUED,
        "CONFIGURING": HpcJobStatus.QUEUED,
        "RUNNING": HpcJobStatus.RUNNING,
        "COMPLETING": HpcJobStatus.COMPLETE,
    }
    _REGEX_SBATCH_OUTPUT = re.compile(r"Submitted batch job (\d+)")

    def __init__(self, config):
        self._config = config

    def am_i_manager(self):
        return os.environ.get("SLURM_NODEID", 1) == "0"

    def cancel_job(self, job_id):
        return run_command(f"scancel {job_id}")

    def check_status(self, name=None, job_id=None):
        field_names = ("jobid", "name", "state")
        cmd = f"squeue -u {self.USER} --Format \"{','.join(field_names)}\" -h"
        if name is not None:
            cmd += f" -n {name}"
        elif job_id is not None:
            cmd += f" -j {job_id}"
        else:
            # Mutual exclusivity should be handled in HpcManager.
            assert False

        output = {}
        # Transient failures could be costly. Retry for up to one minute.
        ret = run_command(cmd, output, num_retries=6, retry_delay_s=10)
        if ret != 0:
            if "Invalid job id specified" in output["stderr"]:
                return HpcJobInfo("", "", HpcJobStatus.NONE)

            logger.error(
                "Failed to run squeue command=[%s] ret=%s err=%s", cmd, ret, output["stderr"]
            )
            raise ExecutionError(f"squeue command failed: {ret}")

        stdout = output["stdout"]
        logger.debug("squeue output:  [%s]", stdout)
        fields = stdout.split()
        if not fields:
            # No jobs are currently running.
            return HpcJobInfo("", "", HpcJobStatus.NONE)

        assert len(fields) == len(field_names)
        job_info = HpcJobInfo(
            fields[0], fields[1], self._STATUSES.get(fields[2], HpcJobStatus.UNKNOWN)
        )
        return job_info

    def check_statuses(self):
        field_names = ("jobid", "state")
        cmd = f"squeue -u {self.USER} --Format \"{','.join(field_names)}\" -h"

        output = {}
        # Transient failures could be costly. Retry for up to one minute.
        ret = run_command(cmd, output, num_retries=6, retry_delay_s=10)
        if ret != 0:
            logger.error(
                "Failed to run squeue command=[%s] ret=%s err=%s", cmd, ret, output["stderr"]
            )
            raise ExecutionError(f"squeue command failed: {ret}")

        return self._get_statuses_from_output(output["stdout"])

    @staticmethod
    def _get_statuses_from_output(output):
        logger.debug("squeue output:  [%s]", output)
        lines = output.split("\n")
        if not lines:
            # No jobs are currently running.
            return {}

        statuses = {}
        for line in lines:
            if line == "":
                continue
            fields = line.strip().split()
            assert len(fields) == 2
            job_id = fields[0]
            status = fields[1]
            statuses[job_id] = SlurmManager._STATUSES.get(status, HpcJobStatus.UNKNOWN)

        return statuses

    @staticmethod
    def check_storage_configuration():
        pass
        # Disabling this code because the Lustre documentation only recommends
        # higher stripe counts when files are large or if many clients will be
        # accessing the files concurrently.
        # JADE shouldn't enforce a single rule for everyone.
        # Leaving the code here in case we want to customize this in the
        # future.
        #
        # References:
        # - http://wiki.lustre.org/Configuring_Lustre_File_Striping
        # - https://www.nics.tennessee.edu/computing-resources/file-systems/lustre-striping-guide

        # output = {}
        # cmd = "lfs getstripe ."
        # ret = run_command(cmd, output)
        # if ret != 0:
        #    raise ExecutionError(f"{cmd} failed: {output}")

        # stripe_count = SlurmManager._get_stripe_count(output["stdout"])
        # logger.info("stripe_count is set to %s", stripe_count)
        # if stripe_count < 16:
        #    raise InvalidConfiguration(
        #        f"stripe_count for {os.getcwd()} is set to {stripe_count}. "
        #        "The runtime directory should be set with a stripe_count of "
        #        "16 for optimal performance. Create a new directory, run "
        #        "`lfs setstripe -c 16 <dirname>`, and then move all contents "
        #        "to that directory."
        #    )

    def get_config(self):
        return self._config

    @staticmethod
    def _get_stripe_count(output):
        regex = re.compile(r"stripe_count:\s+(\d+)")
        match = regex.search(output)
        assert match, output["stdout"]
        return int(match.group(1))

    def create_cluster(self):
        logger.debug("config=%s", self._config)
        assert False, "not supported"
        # cluster = SLURMCluster(
        #    project=self._config["hpc"]["allocation"],
        #    walltime=self._config["hpc"]["walltime"],
        #    job_mem=str(self._config["hpc"]["mem"]),
        #    memory=str(self._config["hpc"]["mem"]) + "MB",
        #    #job_cpu=config["cpu"],
        #    interface=self._config["dask"]["interface"],
        #    local_directory=self._config["dask"]["local_directory"],
        #    cores=self._config["dask"]["cores"],
        #    #processes=config["processes"],
        # )

        # logger.debug("Created cluster. job script %s", cluster.job_script())
        # return cluster

    def create_local_cluster(self):
        assert False, "not supported"
        # cluster = LocalCluster()
        # logger.debug("Created local cluster.")
        # return cluster

    def create_submission_script(self, name, script, filename, path):
        text = self._create_submission_script_text(name, script, path)
        utils.create_script(filename, "\n".join(text))

    def _create_submission_script_text(self, name, script, path):
        lines = [
            "#!/bin/bash",
            f"#SBATCH --account={self._config.hpc.account}",
            f"#SBATCH --job-name={name}",
            f"#SBATCH --time={self._config.hpc.walltime}",
            f"#SBATCH --output={path}/job_output_%j.o",
            f"#SBATCH --error={path}/job_output_%j.e",
        ]

        for param in ("mem", "nodes", "ntasks", "ntasks_per_node", "partition", "qos", "tmp"):
            value = getattr(self._config.hpc, param, None)
            if value is not None:
                lines.append(f"#SBATCH --{param}={value}")

        lines.append("")
        lines.append(f"srun {script}")
        return lines

    def get_local_scratch(self):
        return os.environ["LOCAL_SCRATCH"]

    def get_node_id(self):
        return os.environ["SLURM_NODEID"]

    @staticmethod
    def get_num_cpus():
        return int(os.environ["SLURM_CPUS_ON_NODE"])

    def list_active_nodes(self, job_id):
        out1 = {}
        # It's possible that 500 characters won't be enough, even with the compact format.
        # Compare the node count against the result to make sure we got all nodes.
        # There should be a better way to get this.
        check_run_command(f'squeue -j {job_id} --format="%5D %500N" -h', out1)
        result = out1["stdout"].strip().split()
        assert len(result) == 2, str(result)
        num_nodes = int(result[0])
        nodes_compact = result[1]
        out2 = {}
        check_run_command(f'scontrol show hostnames "{nodes_compact}"', out2)
        nodes = [x for x in out2["stdout"].split("\n") if x != ""]
        if len(nodes) != num_nodes:
            raise Exception(f"Bug in parsing node names. Found={len(nodes)} Actual={num_nodes}")
        return nodes

    def log_environment_variables(self):
        data = {}
        for name, value in os.environ.items():
            if "SLURM" in name:
                data[name] = value

        logger.info("SLURM environment variables: %s", data)

    def submit(self, filename):
        job_id = None
        output = {}
        # Transient failures could be costly. Retry for up to one minute.
        # TODO: Some errors are not transient. We could detect those and skip the retries.
        ret = run_command("sbatch {}".format(filename), output, num_retries=6, retry_delay_s=10)
        if ret == 0:
            result = Status.GOOD
            stdout = output["stdout"]
            match = self._REGEX_SBATCH_OUTPUT.search(stdout)
            if match:
                job_id = match.group(1)
                result = Status.GOOD
            else:
                logger.error("Failed to interpret sbatch output [%s]", stdout)
                result = Status.ERROR
        else:
            result = Status.ERROR

        return result, job_id, output["stderr"]
