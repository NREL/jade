"""Test registry."""

import os
import tempfile

import pytest

from jade.extensions.registry import Registry, ExtensionClassType, DEFAULT_REGISTRY
from jade.extensions.generic_command.generic_command_configuration import (
    GenericCommandConfiguration,
)
from jade.extensions.generic_command.generic_command_execution import GenericCommandExecution
import jade.extensions.generic_command.cli as cli


# Don't change the user's registry.
TEST_FILENAME = os.path.join("tests", "jade_test_registry.json")


@pytest.fixture
def registry_fixture():
    yield
    if os.path.exists(TEST_FILENAME):
        os.remove(TEST_FILENAME)


def clear_extensions(registry):
    for extension in registry.list_extensions():
        registry.unregister_extension(extension["name"])
    assert len(registry.list_extensions()) == 0


def test_registry__list_extensions(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY["extensions"])


def test_registry__unregister_extensions(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY["extensions"])
    clear_extensions(registry)


def test_registry__register_extensions(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    clear_extensions(registry)
    extension = DEFAULT_REGISTRY["extensions"][0]
    registry.register_extension(extension)
    extensions = registry.list_extensions()
    assert len(extensions) == 1
    ext = extensions[0]

    assert ext["name"] == extension["name"]
    cfg_class = registry.get_extension_class(ext["name"], ExtensionClassType.CONFIGURATION)
    assert cfg_class == GenericCommandConfiguration
    exec_class = registry.get_extension_class(ext["name"], ExtensionClassType.EXECUTION)
    assert exec_class == GenericCommandExecution
    cli_mod = registry.get_extension_class(ext["name"], ExtensionClassType.CLI)
    assert cli_mod == cli

    # Test that the the changes are reflected with a new instance.
    registry2 = Registry(registry_filename=TEST_FILENAME)
    extensions1 = registry.list_extensions()
    extensions2 = registry2.list_extensions()
    for ext1, ext2 in zip(extensions1, extensions2):
        for field in DEFAULT_REGISTRY["extensions"][0]:
            assert ext1[field] == ext2[field]


def test_registry__is_registered(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    registry.reset_defaults()
    assert registry.is_registered(DEFAULT_REGISTRY["extensions"][0]["name"])


def test_registry__reset_defaults(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    clear_extensions(registry)
    registry.reset_defaults()
    assert len(registry.list_extensions()) == len(DEFAULT_REGISTRY["extensions"])
    assert registry.list_loggers() == DEFAULT_REGISTRY["logging"]


def test_registry__add_logger(registry_fixture):
    registry = Registry(registry_filename=TEST_FILENAME)
    registry.reset_defaults()
    package = "test-package"
    registry.add_logger(package)
    assert package in registry.list_loggers()
    registry.remove_logger(package)
    assert package not in registry.list_loggers()


def test_registry__show_extensions(capsys, registry_fixture):
    """Test functionality of show_extensions."""
    registry = Registry(registry_filename=TEST_FILENAME)
    registry.reset_defaults()
    registry.show_extensions()
    captured = capsys.readouterr()
    for extension in DEFAULT_REGISTRY["extensions"]:
        assert extension["name"] in captured.out
