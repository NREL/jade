"""Manages extensions registered with JADE."""

import copy
import enum
import importlib
import logging
import os
import pathlib

from jade.exceptions import InvalidParameter
from jade.utils.utils import dump_data, load_data


DEFAULT_REGISTRY = [
    {
        "name": "demo",
        "description": "Country based GDP auto-regression analysis",
        "job_execution_module": "jade.extensions.demo.autoregression_execution",
        "job_execution_class": "AutoRegressionExecution",
        "job_configuration_module": "jade.extensions.demo.autoregression_configuration",
        "job_configuration_class": "AutoRegressionConfiguration",
        "cli_module": "jade.extensions.demo.cli",
    }
]


class ExtensionClassType(enum.Enum):
    """Possible values for computational sequencing mode"""
    CLI = "cli_module"
    CONFIGURATION = "config_class"
    EXECUTION = "exec_class"


logger = logging.getLogger(__name__)


class Registry:
    """Manages extensions registered with JADE."""
    _REGISTRY_FILENAME = ".jade-registry.json"

    def __init__(self, registry_filename=None):
        if registry_filename is None:
            self._registry_filename = os.path.join(
                str(pathlib.Path.home()),
                self._REGISTRY_FILENAME,
            )
        else:
            self._registry_filename = registry_filename

        self._extensions = {}
        if not os.path.exists(self._registry_filename):
            self.reset_defaults()
        else:
            for extension in load_data(self._registry_filename):
                self._add_extension(extension)

    def _add_extension(self, extension):
        for field in DEFAULT_REGISTRY[0]:
            if field not in extension:
                raise InvalidParameter(f"required field {field} not present")

        cmod = importlib.import_module(extension["job_configuration_module"])
        emod = importlib.import_module(extension["job_execution_module"])
        cli_mod = importlib.import_module(extension["cli_module"])

        ext = copy.copy(extension)
        ext[ExtensionClassType.CONFIGURATION] = getattr(
            cmod, extension["job_configuration_class"])
        ext[ExtensionClassType.EXECUTION] = getattr(
            emod, extension["job_execution_class"])
        ext[ExtensionClassType.CLI] = cli_mod

        cfg_module = importlib.import_module(ext["job_execution_module"])
        self._extensions[extension["name"]] = ext

    def _serialize_extensions(self):
        data = []
        for _, extension in sorted(self._extensions.items()):
            ext = {k: v for k, v in extension.items()
                   if not isinstance(k, ExtensionClassType)}
            data.append(ext)

        filename = self.registry_filename
        dump_data(data, filename, indent=4)
        logger.debug("Serialized data to %s", filename)

    def get_extension_class(self, extension_name, class_type):
        """Get the class associated with the extension.

        Parameters
        ----------
        extension_name : str
        class_type : ExtensionClassType

        Raises
        ------
        InvalidParameter
            Raised if the extension is not registered.

        """
        extension = self._extensions.get(extension_name)
        if extension is None:
            raise InvalidParameter(f"{extension_name} is not registered")

        return extension[class_type]

    def is_registered(self, extension_name):
        """Check if the extension is registered"""
        return extension_name in self._extensions

    def list_extensions(self):
        """Return a list of registered extensions.

        Returns
        -------
        list of dict

        """
        return list(self._extensions.values())

    def register_extension(self, extension):
        """Registers an extension in the registry.

        Parameters
        ----------
        extension : dict

        Raises
        ------
        InvalidParameter
            Raised if the extension is invalid.

        """

        self._add_extension(extension)
        self._serialize_extensions()
        logger.debug("Registered extension %s", extension["name"])

    @property
    def registry_filename(self):
        """Return the filename that stores the registry."""
        return self._registry_filename

    def reset_defaults(self):
        """Reset the registry to its default values."""
        self._extensions.clear()
        for extension in DEFAULT_REGISTRY:
            self.register_extension(extension)
        self._serialize_extensions()

        logger.debug("Initialized registry to its defaults.")

    def show_extensions(self):
        """Show the registered extensions."""
        print("JADE Extensions:")
        for name, extension in sorted(self._extensions.items()):
            print(f"  {name}:  {extension['description']}")

    def unregister_extension(self, extension_name):
        """Unregisters an extension.

        Parameters
        ----------
        extension_name : str

        """
        if extension_name not in self._extensions:
            raise InvalidParameter(
                f"extension {extension_name} isn't registered"
            )

        self._extensions.pop(extension_name)
        self._serialize_extensions()
