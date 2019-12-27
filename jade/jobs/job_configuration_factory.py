""""Factory functions for simulation configurations"""

from jade.exceptions import InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.result import ResultsSummary
from jade.utils.utils import load_data
from jade.utils.timing_utils import timed_debug


@timed_debug
def create_config_from_file(filename, **kwargs):
    """Create instance of a JobConfiguration from a config file.

    Returns
    -------
    JobConfiguration

    """
    data = load_data(filename)
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
    allowed_types = ["successful", "failed"]
    if result_type not in allowed_types:
        raise InvalidParameter(f"given result type invalid: {result_type}")

    config = deserialize_config(load_data(config_file))
    summary = ResultsSummary(output)
    results_of_type = []

    if result_type == "successful":
        results_of_type = summary.get_successful_results()
    elif result_type == "failed":
        results_of_type = summary.get_failed_results()

    parameters = []
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
    extension = data["extension"]
    cls = registry.get_extension_class(
        extension, ExtensionClassType.CONFIGURATION)
    return cls.deserialize(data, **kwargs)
