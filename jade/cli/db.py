"""
CLI to ingest results into a SQLite database.
"""

import copy
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import click

from jade.common import CONFIG_FILE, OUTPUT_DIR, RESULTS_FILE, RESULTS_DB_FILE, STATS_SUMMARY_FILE
from jade.jobs.job_configuration_factory import create_config_from_file
from jade.loggers import setup_logging
from jade.utils.sql import make_table, insert_rows
from jade.utils.utils import load_data


@click.group()
def db():
    """Database group commands"""
    setup_logging("db", None)


@click.argument("output", callback=lambda _, __, x: Path(x))
@click.command()
def ingest_results(output):
    """Ingest results from a run into a SQLite database."""
    results_file = output / RESULTS_FILE
    if not results_file.exists():
        print(f"There is no JADE results file in {output}.", file=sys.stderr)
        sys.exit(1)

    # config = create_config_from_file(output / CONFIG_FILE)
    db_file = Path(output) / RESULTS_DB_FILE
    if db_file.exists():
        db_file.unlink()

    create_result_table(db_file, results_file)

    stats_file = output / STATS_SUMMARY_FILE
    if stats_file.exists():
        create_resource_stats_table(db_file, stats_file)
    else:
        print(
            f"Cannot add resource stats to database; {stats_file} does not exist", file=sys.stderr
        )

    events_dir = output / "events"
    if events_dir.exists():
        for event_file in events_dir.iterdir():
            if event_file.suffix == ".json":
                create_event_table(db_file, event_file)
    else:
        print(f"Cannot add events to database; {events_dir} does not exist", file=sys.stderr)


def create_result_table(db_file, results_file):
    results = load_data(results_file)
    if not results:
        print(f"There are no results in {results_file}", file=sys.stderr)
        return

    rows = []
    for result in results["results"]:
        result["completion_time"] = datetime.fromtimestamp(result["completion_time"])
        rows.append(tuple(result.values()))

    for name in results["missing_jobs"]:
        row = (name, None, "missing", None, None, None)
        if len(row) != len(rows[1]):
            print(
                f"BUG: Mismatch in row length for missing job: {name}. This function may not "
                "match the definition of the Result type.",
                file=sys.stderr,
            )
            sys.exit(1)
        rows.append(row)

    table = "result"
    make_table(db_file, table, results["results"][0], primary_key="name")
    insert_rows(db_file, table, rows)


def create_resource_stats_table(db_file, stats_file):
    stats = load_data(stats_file)
    if not stats:
        print(f"There are no results in {stats_file}", file=sys.stderr)
        return

    rows_by_table = defaultdict(list)
    schema_by_table = {}
    for item in stats:
        table = item["type"].lower()
        base_row = {}
        if table == "process":
            # Make this column be listed first.
            base_row["name"] = item["name"]
        base_row["source"] = item["batch"]
        for stat_key in ("average", "minimum", "maximum"):
            stat_data = item[stat_key]
            if stat_data:
                row = copy.deepcopy(base_row)
                row["stat"] = stat_key
                for key, val in stat_data.items():
                    row[key] = val
                if table not in schema_by_table:
                    schema_by_table[table] = {}
                    fixed_columns = {}
                    for key, val in row.items():
                        for illegal in (" ", "-", "/"):
                            key = key.replace(illegal, "_")
                        fixed_columns[key] = val
                    schema_by_table[table] = fixed_columns
                rows_by_table[table].append(tuple(row.values()))

    for table, rows in rows_by_table.items():
        make_table(db_file, table, schema_by_table[table])
        insert_rows(db_file, table, rows)


def create_event_table(db_file, event_file):
    events = load_data(event_file)
    if not events:
        print(f"There are no results in {event_file}", file=sys.stderr)
        return

    rows = []
    schema = None
    table = None
    for event in events:
        event.pop("event_class")
        event.pop("category")
        data = event.pop("data")
        event.update(data)
        if schema is None:
            schema = event
            table = event["name"]
        event.pop("name")
        rows.append(tuple(event.values()))

    make_table(db_file, table, schema)
    insert_rows(db_file, table, rows)


db.add_command(ingest_results)
