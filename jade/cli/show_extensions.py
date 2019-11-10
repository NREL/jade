"""CLI to show extensions."""

import click

import jade.extensions.registry as registry


@click.command()
def show_extensions():
    """Show the available extensions (job types)."""
    registry.show_extensions()
