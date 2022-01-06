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

    collect_worker_logs: bool = Field(
        title="collect_worker_logs",
        description="Collect logs from worker processes.",
        default=False,
    )
    conf_dir: str = Field(
        title="conf_dir",
        description="Spark configuration directory",
        default="spark-conf",
    )
    container: SparkContainerModel = Field(
        title="container",
        description="Container parameters",
    )
    enabled: bool = Field(
        title="enabled",
        description="Set to true to run this job on a Spark cluster",
        default=False,
    )
    master_node_memory_overhead_gb: int = Field(
        title="master_node_memory_overhead_gb",
        description="Memory overhead for Spark master processes",
        default=3,
    )
    node_memory_overhead_gb: int = Field(
        title="node_memory_overhead_gb",
        description="Memory overhead for node operating system and existing applications",
        default=10,
    )
    run_user_script_outside_container: bool = Field(
        title="run_user_script_outside_container",
        description="Run the user script outside of the container.",
        default=False,
    )
    use_tmpfs_for_scratch: bool = Field(
        title="use_tmpfs_for_scratch",
        description="Use node's tmpfs instead of internal storage for scratch space.",
        default=False,
    )
    worker_memory_gb: int = Field(
        title="worker_memory_gb",
        description="If 0, give all node memory minus overhead to worker.",
        default=0,
    )

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
        assert not self.run_user_script_outside_container
        return Path(self.conf_dir) / "bin" / "run_user_script_wrapper.sh"
