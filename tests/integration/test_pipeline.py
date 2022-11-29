"""Pipeline tests"""

import shutil
import tempfile
from pathlib import Path

import pytest

from jade.extensions.generic_command import GenericCommandParameters
from jade.extensions.generic_command import GenericCommandConfiguration
from jade.result import ResultsSummary
from jade.utils.subprocess_manager import run_command


@pytest.fixture
def cleanup():
    tmpdir = Path(tempfile.gettempdir()) / "jade_pipeline_test"
    if tmpdir.exists():
        shutil.rmtree(tmpdir)
    tmpdir.mkdir()
    yield tmpdir
    shutil.rmtree(tmpdir)


def test_pipeline(cleanup):
    tmpdir = Path(cleanup)
    config_files = []
    num_configs = 2
    num_jobs = 2
    for i in range(1, num_configs + 1):
        config = GenericCommandConfiguration()
        for j in range(num_jobs):
            job = GenericCommandParameters(command="echo hello")
            config.add_job(job)
        config_file = tmpdir / f"config_{i}.json"
        config.dump(config_file)
        config_files.append(config_file)

    pipeline_config_file = tmpdir / "pipeline.json"
    cmd = f"jade pipeline create -f {config_files[0]} -f {config_files[1]} -c {pipeline_config_file} -l"
    assert run_command(cmd) == 0
    output = tmpdir / "output"
    submit_cmd = f"jade pipeline submit {pipeline_config_file} -o {output}"
    assert run_command(submit_cmd) == 0

    for i in range(1, num_configs + 1):
        results = ResultsSummary(output / f"output-stage{i}").list_results()
        assert len(results) == num_jobs
        for result in results:
            assert result.return_code == 0
