"""Test registry."""

import atexit
import os
import tempfile

from jade.extensions.registry import Registry, ExtensionClassType, \
    DEFAULT_REGISTRY
from jade.extensions.demo.autoregression_configuration import \
    AutoRegressionConfiguration
from jade.extensions.demo.autoregression_execution import \
    AutoRegressionExecution
import jade.extensions.demo.cli as cli


# Don't change the user's registry.
Registry._PATH = tempfile.gettempdir()
Registry._REGISTRY_FILENAME = "jade_test_registry.json"


def cleanup():
    filename = os.path.join(Registry._PATH, Registry._REGISTRY_FILENAME)
    if os.path.exists(filename):
        os.remove(filename)


atexit.register(cleanup)


def clear_extensions(registry):
    for extension in registry.list_extensions():
        registry.unregister_extension(extension["name"])
    assert len(registry.list_extensions()) == 0


def test_registry__list_extensions():
    registry = Registry()
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY)


def test_registry__unregister_extensions():
    registry = Registry()
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY)
    clear_extensions(registry)


def test_registry__register_extensions():
    registry = Registry()
    clear_extensions(registry)
    extension = DEFAULT_REGISTRY[0]
    registry.register_extension(extension)
    extensions = registry.list_extensions()
    assert len(extensions) == 1
    ext = extensions[0]

    assert ext["name"] == extension["name"]
    cfg_class = registry.get_extension_class(
        ext["name"], ExtensionClassType.CONFIGURATION)
    assert cfg_class == AutoRegressionConfiguration
    exec_class = registry.get_extension_class(
        ext["name"], ExtensionClassType.EXECUTION)
    assert exec_class == AutoRegressionExecution
    cli_mod = registry.get_extension_class(ext["name"], ExtensionClassType.CLI)
    assert cli_mod == cli

    # Test that the the changes are reflected with a new instance.
    registry2 = Registry()
    extensions1 = registry.list_extensions()
    extensions2 = registry2.list_extensions()
    for ext1, ext2 in zip(extensions1, extensions2):
        for field in DEFAULT_REGISTRY[0]:
            assert ext1[field] == ext2[field]


def test_registry__is_registered():
    registry = Registry()
    registry.reset_defaults()
    assert registry.is_registered(DEFAULT_REGISTRY[0]["name"])


def test_registry__reset_defaults():
    registry = Registry()
    clear_extensions(registry)
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY)


def test_registry__show_extensions(capsys):
    """Test functionality of show_extensions."""
    registry = Registry()
    registry.reset_defaults()
    registry.show_extensions()
    captured = capsys.readouterr()
    for extension in DEFAULT_REGISTRY:
        assert extension["name"] in captured.out
