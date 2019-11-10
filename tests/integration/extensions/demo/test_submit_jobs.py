"""
Integration tests for submitting jobs for auto-regression analysis.
"""
import os
import pathlib
import shutil
from jade.utils.subprocess_manager import run_command


def test_submit_jobs(test_data_dir):
    """Should submit jobs successfully and return expected results"""
    inputs = os.path.join(test_data_dir, "demo")
    config_file = os.path.join(inputs, "test-config.json")

    assert os.path.exists(config_file)

    proj_dir = pathlib.Path(__file__).parents[4]
    hpc_config_file = os.path.join(proj_dir, "hpc_config.toml")
    cmd = "jade submit-jobs {} -h {}".format(config_file, hpc_config_file)

    output = os.path.join(test_data_dir, "output")
    if os.path.exists(output):
        shutil.rmtree(output)

    returncode = run_command(cmd=cmd, cwd=test_data_dir)
    assert returncode == 0

    output = os.path.join(test_data_dir, "output")
    assert os.path.exists(output)

    job_outputs = os.path.join(output, "job-outputs")

    for country in ("brazil", "united_states"):
        results = os.listdir(os.path.join(job_outputs, country))
        assert "result.csv" in results
        assert "result.png" in results
        assert "summary.toml" in results

    if os.path.exists(output):
        shutil.rmtree(output)
