"""CLI to display and manage config files."""

import logging
import os
import re
import sys
import tempfile

import click
from prettytable import PrettyTable

from jade.common import CONFIG_FILE
from jade.extensions.generic_command.generic_command_configuration import GenericCommandConfiguration
from jade.loggers import setup_logging
from jade.utils.utils import dump_data, load_data


logger = logging.getLogger(__name__)


@click.group()
def config():
    """Manage a JADE configuration."""
    setup_logging("config", None)


@click.command()
@click.argument("filename", type=click.Path(exists=True))
@click.option(
    "-c",
    "--config-file",
    default=CONFIG_FILE,
    show_default=True,
    help="config file to generate.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def create(filename, config_file, verbose):
    """Create a config file from a filename with a list of executable commands."""
    level = logging.DEBUG if verbose else logging.WARNING
    setup_logging("auto_config", None, console_level=level)

    config = GenericCommandConfiguration.auto_config(filename)
    print(f"Created configuration with {config.get_num_jobs()} jobs.")
    config.dump(config_file)
    print(f"Dumped configuration to {config_file}.\n")


@click.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "-f", "--fields",
    multiple=True,
    help="include in output table; can specify mulitple times",
)
def show(config_file, fields):
    """Show the jobs in the configuration."""
    _show(config_file, fields)


# This is a standalone function so that it can be called from _filter.
def _show(config_file, fields):
    cfg = load_data(config_file)
    jobs = cfg["jobs"]
    print(f"Extension: {cfg['extension']}")
    print(f"Num jobs: {len(cfg['jobs'])}")
    if not jobs:
        return

    for field in fields:
        if field not in jobs[0]:
            print(f"field={field} is not a job field in {cfg['extension']}")
            sys.exit(1)

    field_names = ["index"]
    if "name" in jobs[0]:
        field_names.append("name")
    else:
        field_names.append(list(jobs[0].keys())[0])
    if "blocked_by" in jobs[0]:
        field_names.append("blocked_by")

    table = PrettyTable()
    table.field_names = field_names + list(fields)
    for i, job in enumerate(jobs):
        row = [i] + [job[x] for x in field_names[1:]]
        table.add_row(row)
    print(table)


@click.command("filter")
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("indices", nargs=-1)
@click.option(
        "-o", "--output-file",
        help="Create new config file with filtered jobs.",
)
@click.option(
    "-f", "--fields",
    type=str,
    multiple=True,
    nargs=2,
    help="Filter on field value. Multiple accepted.",
)
@click.option(
    "-s", "--show-config",
    is_flag=True,
    show_default=True,
    default=False,
    help="Show the new config (only applicable if output-file is provided).",
)
# Named _filter to avoid collisions with the built-in function.
def _filter(config_file, output_file, indices, fields, show_config=False):
    """Filters jobs in CONFIG_FILE. Prints the new jobs to the console or
    optionally creates a new file.

    Note: This does not detect duplicate ranges.

    \b
    Examples:
    1. Select the first job. Output only.
       jade config filter c1.json 0
    2. Select indices 0-4, 10-14, 20, 25, create new file.
       jade config filter c1.json :5 10:15 20 25 -o c2.json
    3. Select the last 5 jobs. Note the use of '--' to prevent '-5' from being
       treated as an option.
       jade config filter c1.json -o c2.json -- -5:
    4. Select indices 5 through the end.
       jade config filter c1.json -o c2.json 5:
    5. Select jobs with parameters param1=green and param2=3.
       jade config filter c1.json -o c2.json -f param1 green -f param2 3

    """
    cfg = load_data(config_file)
    jobs = cfg["jobs"]
    if not jobs:
        print("The configuration has no jobs")
        sys.exit(1)

    if output_file is None:
        handle, new_config_file = tempfile.mkstemp(suffix=".json")
        os.close(handle)
        show_config = True
    else:
        new_config_file = output_file

    try:
        if not new_config_file.endswith(".json"):
            print("new_config_file must have extension .json")
            sys.exit(1)

        orig_len = len(jobs)
        new_jobs = []
        regex_int = re.compile(r"^(?P<index>\d+)$")
        regex_range = re.compile(r"^(?P<start>[\d-]*):(?P<end>[\d-]*)$")
        for index in indices:
            match = regex_int.search(index)
            if match:
                i = int(match.groupdict()["index"])
                new_jobs.append(jobs[i])
                continue
            match = regex_range.search(index)
            if match:
                start = match.groupdict()["start"]
                if start == "":
                    start = None
                else:
                    start = int(start)
                end = match.groupdict()["end"]
                if end == "":
                    end = None
                else:
                    end = int(end)
                new_jobs += jobs[start:end]

        # Note: when looking at just the JSON, there is no way to get the job name,
        # and so we can't check for duplicates.

        if not new_jobs:
            new_jobs = jobs

        if fields:
            final_jobs = []
            for job in new_jobs:
                matched = True
                for field in fields:
                    if str(job[field[0]]) != field[1]:
                        matched = False
                        break
                if matched:
                    final_jobs.append(job)

            new_jobs = final_jobs

        cfg["jobs"] = new_jobs
        new_len = len(cfg["jobs"])
        dump_data(cfg, new_config_file, indent=4)
        print(f"Filtered {config_file} ({orig_len} jobs) into ({new_len} jobs)\n")
        if output_file is not None:
            print(f"Wrote new config to {output_file}")

        if show_config:
            _show(new_config_file, [])
    finally:
        if output_file is None:
            os.remove(new_config_file)


config.add_command(create)
config.add_command(show)
config.add_command(_filter)
