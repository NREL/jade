""""Factory functions for simulation configurations"""

from jade.extensions.registry import Registry, ExtensionClassType
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
    registry = Registry()
    extension = data["extension"]
    cls = registry.get_extension_class(
        extension, ExtensionClassType.CONFIGURATION)
    return cls.deserialize(data, **kwargs)
