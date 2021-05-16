""""Factory functions for simulation configurations"""

import logging

from jade.exceptions import InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.jobs.job_configuration import JobConfiguration
from jade.result import ResultsSummary
from jade.utils.utils import dump_data, load_data
from jade.utils.timing_utils import timed_debug


logger = logging.getLogger(__name__)


@timed_debug
def create_config_from_file(filename, **kwargs):
    """Create instance of a JobConfiguration from a config file.

    Returns
    -------
    JobConfiguration

    """
    data = load_data(filename)
    format = data.get("format_version", None)
    if format is None:
        upgrade_config_file(data, filename)

    return deserialize_config(data, **kwargs)


@timed_debug
def create_config_from_previous_run(config_file, output, result_type="successful", **kwargs):
    """Create instance of a JobConfiguration from a previous config file,
    returning only those of the type given

    Parameters
    ----------
    config_file : str
        location of config
    output : str
        location of previous results
    result_type : string
        type of results

    Returns
    -------
    JobConfiguration

    Raises
    ------
    InvalidParameter
            Raised if result_type is not successful or failed

    """
    allowed_types = ["successful", "failed", "missing"]
    if result_type not in allowed_types:
        raise InvalidParameter(f"given result type invalid: {result_type}")

    config = deserialize_config(load_data(config_file))
    summary = ResultsSummary(output)
    results_of_type = []

    if result_type == "successful":
        results_of_type = summary.get_successful_results()
    elif result_type == "failed":
        results_of_type = summary.get_failed_results()
    elif result_type == "missing":
        results_of_type = summary.get_missing_jobs(config.iter_jobs())

    parameters = []
    # Note that both jobs and results have `.name`.
    for result in results_of_type:
        job_parameters = config.get_job(result.name)
        parameters.append(job_parameters)

    config.reconfigure_jobs(parameters)
    return deserialize_config(config.serialize(), **kwargs)


@timed_debug
def deserialize_config(data, **kwargs):
    """Create instance of a JobConfiguration from a dict.

    Parameters
    ----------
    data : dict
        Dictionary loaded from a serialized config file.

    Returns
    -------
    JobConfiguration

    """
    registry = Registry()
    config_module = data["configuration_module"]
    config_class = data["configuration_class"]
    for ext in registry.iter_extensions():
        ext_cfg_class = ext[ExtensionClassType.CONFIGURATION]
        if ext_cfg_class.__module__ == config_module and ext_cfg_class.__name__ == config_class:
            return ext_cfg_class.deserialize(data, **kwargs)

    raise InvalidParameter(f"Cannot deserialize {config_module}.{config_class}")


def upgrade_config_file(data, filename):
    """Upgrades v0.1.0 format to the latest."""
    if data["class"] != "GenericCommandConfiguration":
        raise Exception(f"{filename} has an old format and must be regenerated")

    data["configuration_module"] = "jade.extensions.generic_command.generic_command_configuration"
    data["configuration_class"] = "GenericCommandConfiguration"
    data["format_version"] = JobConfiguration.FORMAT_VERSION
    data.pop("class")
    data.pop("extension")
    for job in data["jobs"]:
        job["extension"] = "generic_command"
        job["append_output_dir"] = False
    dump_data(data, filename, indent=2)
    logger.info("Upgraded config file format: %s", filename)
