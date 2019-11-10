"""
Integration test for auto config auto-regression analysis jobs
"""
import os
from jade.utils.utils import load_data
from jade.utils.subprocess_manager import run_command


def test_auto_config(test_data_dir):
    """Should create a config.json file"""
    inputs = os.path.join(test_data_dir, "demo")
    config_file = os.path.join(inputs, "config.json")

    if os.path.exists(config_file):
        os.remove(config_file)

    # run command
    cmd = "jade auto-config demo {} -c {}".format(inputs, config_file)
    returncode = run_command(cmd=cmd)
    assert returncode == 0
    assert os.path.exists(config_file)

    # check result
    data = load_data(config_file)

    assert "jobs" in data
    assert len(data["jobs"]) == 3

    if os.path.exists(config_file):
        os.remove(config_file)
