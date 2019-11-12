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
    extensions = load_data(extension_file)
    registry = Registry()
    for extension in extensions:
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
def show():
    """Show the available extensions (job types)."""
    Registry().show_extensions()


extensions.add_command(register)
extensions.add_command(reset_defaults)
extensions.add_command(unregister)
extensions.add_command(show)
