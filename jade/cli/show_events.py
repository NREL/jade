"""
CLI to show events of a scenario.
"""

import logging

import click

from jade.common import OUTPUT_DIR
from jade.loggers import setup_logging
from jade.events import EventsSummary


@click.argument("names", nargs=-1)
@click.option("-o", "--output", default=OUTPUT_DIR, show_default=True, help="Output directory.")
@click.option(
    "-c",
    "--categories",
    is_flag=True,
    default=False,
    show_default=True,
    help="Treat the arguments as categories instead of names.",
)
@click.option(
    "-j",
    "--json-fmt",
    is_flag=True,
    default=False,
    show_default=True,
    help="Print event in JSON format, instead of table",
)
@click.option(
    "-n",
    "--names-only",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show event names in output.",
)
@click.option(
    "-C",
    "--categories-only",
    is_flag=True,
    default=False,
    show_default=True,
    help="Show event categories in output.",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose event outputs.",
)
@click.command()
def show_events(
    output,
    names,
    categories=False,
    json_fmt=False,
    names_only=False,
    categories_only=False,
    verbose=False,
):
    """Shows the events after jobs run.

    \b
    Examples:
    jade show-events
    jade show-events unhandled_error
    jade show-events error -c
    jade show-events --names-only
    jade show-events --categories-only
    """
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("show_results", None, console_level=level)
    results = EventsSummary(output)
    if names_only:
        results.show_event_names()
    elif categories_only:
        results.show_event_categories()
    elif json_fmt:
        print(results.to_json())
    else:
        if not names:
            names = results.list_unique_names()
        for name in names:
            if categories:
                results.show_events_in_category(name)
            else:
                results.show_events(name)
