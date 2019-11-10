"""
Unit tests for dss utility functions
"""
from mock import mock_open, patch
import os
import pytest
from jade.utils.dss_utils import *


@pytest.fixture
def opendss_event_log_file(test_data_dir):
    """Fixture of OpenDss event log file"""
    return os.path.join(test_data_dir, "dss_event_log.csv")


def test_get_pv_controllers():
    """Should parse PV controllers specified in OpenDSS deployment file"""

    test_file = "/opendss/deployment/file"
    data = "New PVSystem.pv_1114018 bus1=133294_xfmr.1.2 phases=2"
    with patch("builtins.open", mock_open(read_data=data)) as mock_file:
        controllers = get_pv_controllers(test_file)
        mock_file.assert_called_with(test_file)
        assert len(controllers) == 1
        assert controllers[0] == "pv_1114018"


def test_read_capacitor_changes(opendss_event_log_file):
    """Should parse the capacitor state changes from an OpenDSS event log"""
    data = read_capacitor_changes(opendss_event_log_file)
    assert data == {
        'Capacitor.1809_0_2864_0': 4,
        'Capacitor.1809_0_2703_0': 4,
        'Capacitor.1809_0_2701_0': 4,
    }


def test_read_event_log(opendss_event_log_file):
    """Should return OpenDSS event log information"""
    data = read_event_log(opendss_event_log_file)
    assert len(data) == 29
    for row in data:
        for field in ("Element", "Action"):
            assert field in row
