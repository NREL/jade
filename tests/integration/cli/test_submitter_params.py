from pathlib import Path
from tempfile import gettempdir

import pytest

from jade.enums import ResourceMonitorType
from jade.utils.subprocess_manager import check_run_command
from jade.utils.utils import load_data


CMD = "jade config submitter-params -h tests/data/fake_hpc_config.toml"


@pytest.fixture
def config_file():
    path = Path(gettempdir()) / "test-params.json"
    if path.exists():
        path.unlink()
    yield path
    if path.exists():
        path.unlink()


def test_defaults(config_file):
    check_run_command(f"{CMD} -c {config_file}")
    assert config_file.exists()
    assert load_data(config_file)["resource_monitor_type"] == ResourceMonitorType.AGGREGATION.value


def test_custom_interval(config_file):
    check_run_command(f"{CMD} -R aggregation -r 3 -c {config_file}")
    assert config_file.exists()
    data = load_data(config_file)
    assert data["resource_monitor_interval"] == 3
    assert data["resource_monitor_type"] == ResourceMonitorType.AGGREGATION.value


def test_legacy_stats_enabled(config_file):
    check_run_command(f"{CMD} -r 5 -c {config_file}")
    assert config_file.exists()
    data = load_data(config_file)
    assert data["resource_monitor_interval"] == 5
    assert data["resource_monitor_type"] == ResourceMonitorType.PERIODIC.value


def test_none(config_file):
    check_run_command(f"{CMD} -R none -c {config_file}")
    assert config_file.exists()
    data = load_data(config_file)
    assert data["resource_monitor_type"] == ResourceMonitorType.NONE.value


def test_singularity_params(config_file):
    # Not a real container. It needs to be a path that exists
    container = "jade"
    check_run_command(f"{CMD} -S -C {container} -c {config_file}")
    assert config_file.exists()
    data = load_data(config_file)
    assert data["singularity_params"] is not None
    assert data["singularity_params"]["enabled"]
    assert data["singularity_params"]["container"] == "jade"
