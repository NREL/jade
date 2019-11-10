""""Factory functions for simulation configurations"""

import os

from jade.extensions.hosting_capacity_analysis.hosting_capacity_configuration import \
    HostingCapacityConfiguration
from jade.common import CONFIG_FILE
from jade.exceptions import InvalidParameter
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
    if data["class"] == "AutoRegressionConfiguration":
        from jade.extensions.demo.autoregression_configuration import AutoRegressionConfiguration
        config = AutoRegressionConfiguration.deserialize(data, **kwargs)
    else:
        raise InvalidParameter("unsupported class: {}".format(data["class"]))

    return config
