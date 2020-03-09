"""CLI to show extensions."""

import click

from jade.extensions.registry import Registry
from jade.loggers import setup_logging
from jade.utils.utils import load_data


@click.group()
def extensions():
    """Manage JADE extensions."""
    setup_logging("extensions", None)


@click.command()
@click.argument("extension-file")
def register(extension_file):
    """Register one or more extensions."""
    registry = Registry()
    for extension in load_data(extension_file):
        registry.register_extension(extension)


@click.command()
def reset_defaults():
    """Reset registry to its default values."""
    Registry().reset_defaults()


@click.command()
@click.argument("extension")
def unregister(extension):
    """Unregister an extension."""
    registry = Registry()
    registry.unregister_extension(extension)


@click.command()
@click.argument("package-name")
def add_logger(package_name):
    """Add logging for a package."""
    registry = Registry()
    registry.add_logger(package_name)


@click.command()
@click.argument("package-name")
def remove_logger(package_name):
    """Remove logging for a package."""
    registry = Registry()
    registry.remove_logger(package_name)


@click.command()
def show():
    """Show the available extensions (job types)."""
    print("Extensions:")
    Registry().show_extensions()
    print("Logging enabled for packages:  ", end="")
    Registry().show_loggers()


extensions.add_command(register)
extensions.add_command(reset_defaults)
extensions.add_command(unregister)
extensions.add_command(add_logger)
extensions.add_command(remove_logger)
extensions.add_command(show)
