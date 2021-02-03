"""
Unit tests for demo extension CLI functions
"""
import os
import shutil
import tempfile

from mock import patch

from jade.extensions.demo.autoregression_configuration import AutoRegressionConfiguration
from jade.extensions.demo.autoregression_inputs import AutoRegressionInputs
from jade.extensions.demo.cli import auto_config, run


def test_auto_config(test_data_dir):
    """Should return desired auto-regression configuration"""
    inputs = os.path.join(test_data_dir, "demo")
    config = auto_config(inputs)

    assert isinstance(config, AutoRegressionConfiguration)
    assert config.get_num_jobs() == 3


@patch("jade.extensions.demo.cli.create_config_from_file")
@patch("jade.extensions.demo.cli.AutoRegressionExecution")
def test_run(mock_execution_class, mock_config_create_from_file):
    """Should AutoRegressionExecution.run() method be triggered"""
    config_file = "config.json"
    name = "job name"
    output = os.path.join(tempfile.gettempdir(), "jade-unit-test-dir")
    output_format = "csv"
    run(config_file, name, output, output_format, False)
    mock_config_create_from_file.assert_called_once()
    mock_execution_class().run.assert_called_once()

    if os.path.exists(output):
        shutil.rmtree(output)
