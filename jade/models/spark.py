from pathlib import Path
from typing import Dict, List, Set

from pydantic import Field

from jade.models import JadeBaseModel


class SparkContainerModel(JadeBaseModel):
    """Model definition for a Spark container"""

    path: str = Field(
        title="path",
        description="Path to container that can run Spark",
    )


class SparkConfigModel(JadeBaseModel):
    """Model definition for a Spark configuration"""

    conf_dir: str = Field(
        title="conf_dir",
        description="Spark configuration directory",
        default="spark-conf",
    )
    enabled: bool = Field(
        title="enabled",
        description="Set to true to run this job on a Spark cluster",
        default=False,
    )
    container: SparkContainerModel = Field(
        title="container",
        description="Container parameters",
    )
    worker_memory_gb: int = Field(
        title="worker_memory_gb",
        description="Amount of memory to allocate to worker processes",
        default=80,
    )
    # TODO: Add option to configure Spark logging

    def get_spark_script(self):
        wrapper = Path(self.conf_dir) / "bin" / "run_spark_script_wrapper.sh"
        script = Path(self.conf_dir) / "bin" / "run_spark_script.sh"
        return f"{wrapper} {script}"

    def get_start_master(self):
        return self.get_spark_script() + " sbin/start-master.sh"

    def get_stop_master(self):
        return self.get_spark_script() + " sbin/stop-master.sh"

    def get_start_worker(self, memory, cluster):
        return self.get_spark_script() + f" sbin/start-worker.sh -m {memory} {cluster}"

    def get_stop_worker(self):
        return self.get_spark_script() + " sbin/stop-worker.sh"

    def get_start_history_server(self):
        return self.get_spark_script() + " sbin/start-history-server.sh"

    def get_stop_history_server(self):
        return self.get_spark_script() + " sbin/stop-history-server.sh"

    def get_run_user_script(self):
        return Path(self.conf_dir) / "bin" / "run_user_script_wrapper.sh"
