"""Manages extensions registered with JADE."""

import copy
import enum
import importlib
import logging
import os

import jade
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
    _PATH = os.path.join(
        os.path.dirname(getattr(jade, "__path__")[0]),
        "jade",
        "extensions",
    )
    _REGISTRY_FILENAME = "registry.json"

    def __init__(self):
        self._extensions = {}
        filename = self._get_registry_filename()
        if not os.path.exists(filename):
            self.reset_defaults()
        else:
            for extension in load_data(filename):
                self._add_extension(extension)
                self._extensions[extension["name"]] = extension

    @staticmethod
    def _get_registry_filename():
        return os.path.join(
            Registry._PATH, Registry._REGISTRY_FILENAME,
        )

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
            ext = {k: v for k, v in extension.items() if not isinstance(k, ExtensionClassType)}
            #ext = copy.deepcopy(extension)
            # No need to serialize these.
            #for key in ExtensionClassType:
                #ext.pop(key)
            data.append(ext)

        filename = self._get_registry_filename()
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

    def reset_defaults(self):
        """Reset the registry to its default values."""
        self._extensions.clear()
        for extension in DEFAULT_REGISTRY:
            self.register_extension(extension)
        self._serialize_extensions()

        logger.debug("Initialized registry to its defaults.")

    def list_extensions(self):
        """Return a list of registered extensions.

        Returns
        -------
        list of dict

        """
        return list(self._extensions.values())

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
