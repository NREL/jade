"""Manages extensions registered with JADE."""

import copy
import enum
import importlib
import logging
import os
import pathlib

from jade.exceptions import InvalidParameter
from jade.utils.utils import dump_data, load_data


DEFAULT_REGISTRY = {
    "extensions": [
        {
            "name": "generic_command",
            "description": "Allows batching of a list of CLI commands.",
            "job_execution_module": "jade.extensions.generic_command.generic_command_execution",
            "job_execution_class": "GenericCommandExecution",
            "job_configuration_module": "jade.extensions.generic_command.generic_command_configuration",
            "job_configuration_class": "GenericCommandConfiguration",
            "job_parameters_module": "jade.extensions.generic_command.generic_command_parameters",
            "job_parameters_class": "GenericCommandParameters",
            "cli_module": "jade.extensions.generic_command.cli",
        },
    ],
    "logging": [
        "jade",
    ],
}


class ExtensionClassType(enum.Enum):
    """Possible values for computational sequencing mode"""

    CLI = "cli_module"
    CONFIGURATION = "config_class"
    EXECUTION = "exec_class"
    PARAMETERS = "param_class"


logger = logging.getLogger(__name__)


class Registry:
    """Manages extensions registered with JADE."""

    _REGISTRY_FILENAME = ".jade-registry.json"
    FORMAT_VERSION = "v0.2.0"

    def __init__(self, registry_filename=None):
        if registry_filename is None:
            self._registry_filename = os.path.join(
                str(pathlib.Path.home()),
                self._REGISTRY_FILENAME,
            )
        else:
            self._registry_filename = registry_filename

        self._extensions = {}
        self._loggers = set()
        if not os.path.exists(self._registry_filename):
            self.reset_defaults()
        else:
            data = self._check_registry_config(self._registry_filename)
            for extension in data["extensions"]:
                self._add_extension(extension)
            for package_name in data["logging"]:
                self._loggers.add(package_name)

    def _add_extension(self, extension):
        for field in DEFAULT_REGISTRY["extensions"][0]:
            if field not in extension:
                raise InvalidParameter(f"required field {field} not present")

        cmod = importlib.import_module(extension["job_configuration_module"])
        emod = importlib.import_module(extension["job_execution_module"])
        pmod = importlib.import_module(extension["job_parameters_module"])
        cli_mod = importlib.import_module(extension["cli_module"])

        ext = copy.copy(extension)
        ext[ExtensionClassType.CONFIGURATION] = getattr(cmod, extension["job_configuration_class"])
        ext[ExtensionClassType.EXECUTION] = getattr(emod, extension["job_execution_class"])
        ext[ExtensionClassType.PARAMETERS] = getattr(pmod, extension["job_parameters_class"])
        ext[ExtensionClassType.CLI] = cli_mod

        self._extensions[extension["name"]] = ext

    def _check_registry_config(self, filename):
        data = load_data(filename)
        if isinstance(data, list):
            # Workaround to support the old registry format. 03/06/2020
            # It can be removed eventually.
            new_data = {
                "extensions": data,
                "logging": DEFAULT_REGISTRY["logging"],
            }
            dump_data(new_data, self.registry_filename, indent=4)
            print(
                "\nReformatted registry. Refer to `jade extensions --help` "
                "for instructions on adding logging for external packages.\n"
            )
            data = new_data

        format = data.get("format_version", "v0.1.0")
        if format == "v0.1.0":
            self.reset_defaults()
            data = load_data(filename)
            print(
                "\nWARNING: Reformatted registry. You will need to "
                "re-register any external extensions.\n"
            )
        return data

    def _serialize_registry(self):
        data = {
            "extensions": [],
            "logging": list(self._loggers),
            "format_version": self.FORMAT_VERSION,
        }
        for _, extension in sorted(self._extensions.items()):
            ext = {k: v for k, v in extension.items() if not isinstance(k, ExtensionClassType)}
            data["extensions"].append(ext)

        filename = self.registry_filename
        dump_data(data, filename, indent=4)
        logger.debug("Serialized data to %s", filename)

    def add_logger(self, package_name):
        """Add a package name to the logging registry.

        Parameters
        ----------
        package_name : str

        """
        self._loggers.add(package_name)
        self._serialize_registry()

    def remove_logger(self, package_name):
        """Remove a package name from the logging registry.

        Parameters
        ----------
        package_name : str

        """
        self._loggers.remove(package_name)
        self._serialize_registry()

    def list_loggers(self):
        """List the package names registered to be logged.

        Returns
        -------
        list

        """
        return sorted(list(self._loggers))

    def show_loggers(self):
        """Print the package names registered to be logged."""
        print(", ".join(self.list_loggers()))

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

    def iter_extensions(self):
        """Return an iterator over registered extensions.

        Returns
        -------
        dict_values

        """
        return self._extensions.values()

    def list_extensions(self):
        """Return a list of registered extensions.

        Returns
        -------
        list of dict

        """
        return list(self.iter_extensions())

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
        self._serialize_registry()
        logger.debug("Registered extension %s", extension["name"])

    @property
    def registry_filename(self):
        """Return the filename that stores the registry."""
        return self._registry_filename

    def reset_defaults(self):
        """Reset the registry to its default values."""
        self._extensions.clear()
        self._loggers.clear()
        for extension in DEFAULT_REGISTRY["extensions"]:
            self.register_extension(extension)
        for package_name in DEFAULT_REGISTRY["logging"]:
            self.add_logger(package_name)
        self._serialize_registry()

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
            raise InvalidParameter(f"extension {extension_name} isn't registered")

        self._extensions.pop(extension_name)
        self._serialize_registry()

    def register_demo_extension(self):
        self.register_extension(
            {
                "name": "demo",
                "description": "Country based GDP auto-regression analysis",
                "job_execution_module": "jade.extensions.demo.autoregression_execution",
                "job_execution_class": "AutoRegressionExecution",
                "job_configuration_module": "jade.extensions.demo.autoregression_configuration",
                "job_configuration_class": "AutoRegressionConfiguration",
                "job_parameters_module": "jade.extensions.demo.autoregression_parameters",
                "job_parameters_class": "AutoRegressionParameters",
                "cli_module": "jade.extensions.demo.cli",
            },
        )
