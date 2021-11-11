"""Generates metrics from a Spark cluster."""

import logging
import requests
from pathlib import Path

from jade.exceptions import ExecutionError
from jade.utils.utils import dump_data


logger = logging.getLogger(__name__)


class SparkMetrics:
    """Records metrics from a Spark cluster"""

    METRICS_FILE = "metrics.json"

    def __init__(self, manager_node, history=False):
        self._manager_node = manager_node
        port = "18080" if history else "4040"
        self._endpoint = f"http://{manager_node}:{port}/api/v1/applications/"

    def _submit_request(self, cmd, *args):
        if not cmd.endswith("/"):
            cmd += "/"
        if args:
            cmd = cmd + "/".join(args)
        logger.info("Submitting %s", cmd)
        response = requests.get(cmd)
        if response.status_code != 200:
            raise ExecutionError(f"{cmd} failed: status_code={response.status_code}")

        return response.json()

    def generate_metrics(self, output_dir: Path):
        """Generate metrics from a Spark cluster into files at the given path.

        Parameters
        ----------
        cluster: str
            Cluster master node name
        output_dir: Path
            Output directory in which to write files

        """
        output_dir.mkdir(exist_ok=True, parents=True)
        apps = self.list_applications()
        results = {"metrics": []}
        for app in apps:
            metrics = {
                "application": app,
                "executors": self._submit_request(self._endpoint, app["id"], "executors"),
                "jobs": self._submit_request(self._endpoint, app["id"], "jobs"),
            }
            results["metrics"].append(metrics)

        filename = output_dir / self.METRICS_FILE
        dump_data(results, filename, indent=2)
        logger.info("Recorded metrics in %s", filename)

    def list_applications(self):
        """Return the applications in the cluster.

        Returns
        -------
        list
            List of application objects

        """
        return self._submit_request(self._endpoint)
