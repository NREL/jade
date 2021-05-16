"""
This module contains StructuredLogEvent and EventSummary classes.
"""

from collections import defaultdict
import json
import logging
import os
import re
import sys
from datetime import datetime

from prettytable import PrettyTable

from jade.common import JOBS_OUTPUT_DIR
from jade.exceptions import InvalidConfiguration
from jade.utils.utils import dump_data, load_data


EVENT_DIR = "events"

EVENTS_FILENAME = "events.json"

EVENT_CATEGORY_ERROR = "Error"
EVENT_CATEGORY_HPC = "HPC"
EVENT_CATEGORY_RESOURCE_UTIL = "ResourceUtilization"

EVENT_NAME_HPC_SUBMIT = "hpc_submit"
EVENT_NAME_HPC_JOB_ASSIGNED = "hpc_job_assigned"
EVENT_NAME_HPC_JOB_STATE_CHANGE = "hpc_job_state_change"
EVENT_NAME_CPU_STATS = "cpu_stats"
EVENT_NAME_DISK_STATS = "disk_stats"
EVENT_NAME_MEMORY_STATS = "mem_stats"
EVENT_NAME_NETWORK_STATS = "net_stats"
EVENT_NAME_BYTES_CONSUMED = "bytes_consumed"
EVENT_NAME_UNHANDLED_ERROR = "unhandled_error"
EVENT_NAME_ERROR_LOG = "log_error"
EVENT_NAME_SUBMIT_STARTED = "submit_started"
EVENT_NAME_SUBMIT_COMPLETED = "submit_completed"
EVENT_NAME_CONFIG_EXEC_SUMMARY = "config_exec_summary"

logger = logging.getLogger(__name__)


class StructuredLogEvent:
    """
    A class for recording structured log events.
    """

    def __init__(self, source, category, name, message, **kwargs):
        """
        Initialize the class

        Parameters
        ----------
        source: str,
            The source of the event.
        category: str,
            An event category given by the user.
        name: str,
            An event name given by the user.
        message:
            An event message given the user.

        kwargs:
            Other information that the user needs to record into event.
        """
        self.source = source
        self.category = category
        self.name = name
        self.message = message
        self.event_class = self.__class__.__name__

        if "timestamp" in kwargs:
            self.timestamp = kwargs.pop("timestamp")
        else:
            self.timestamp = str(datetime.now())

        self.data = kwargs

    def base_field_names(self):
        """Return the base field names for the event.

        Returns
        -------
        list

        """
        return self._base_field_names()

    @staticmethod
    def _base_field_names():
        return ["timestamp", "source", "message"]

    def field_names(self):
        """Return all field names for the event.

        Returns
        -------
        list

        """
        return self.base_field_names() + list(self.data.keys())

    def values(self):
        """Return the values for all fields in the event.

        Returns
        -------
        list

        """
        # Account for events generated with different versions of code.
        values = [getattr(self, x, "") for x in self.base_field_names()]
        values += [self.data.get(x, "") for x in self.data]
        return values

    @classmethod
    def deserialize(cls, record):
        """Deserialize event from JSON.

        Parameters
        ----------
        record : dict

        Returns
        -------
        StructuredLogEvent

        """
        return cls(
            source=record.get("source", ""),
            category=record.get("category", ""),
            name=record.get("name", ""),
            message=record.get("message", ""),
            timestamp=record.get("timestamp", ""),
            **record["data"],
        )

    def __str__(self):
        """To format a event instance to string"""
        return json.dumps(self.__dict__, sort_keys=True)

    def to_dict(self):
        """Convert event object to dict"""
        return self.__dict__


class StructuredErrorLogEvent(StructuredLogEvent):
    """Event specific to exceptions"""

    def __init__(self, source, category, name, message, **kwargs):
        """Must be called in an exception context."""
        super().__init__(source, category, name, message, **kwargs)
        if "exception" not in kwargs:
            self._parse_traceback()

    def base_field_names(self):
        return self._base_field_names()

    def _parse_traceback(self):
        """
        Parse the system exception information - exception, filename, and lineno.
        """
        exc_type, exc_obj, tb = sys.exc_info()
        assert tb is not None, "must be called in an exception context"

        self.data["exception"] = str(exc_type)
        self.data["error"] = str(exc_obj).strip()
        self.data["filename"] = os.path.basename(tb.tb_frame.f_code.co_filename)
        self.data["lineno"] = tb.tb_lineno


def deserialize_event(data):
    """Construct an event from raw  data.

    Parameters
    ----------
    data : dict

    Returns
    -------
    StructuredLogEvent

    """
    if data["event_class"] == "StructuredLogEvent":
        return StructuredLogEvent.deserialize(data)
    if data["event_class"] == "StructuredErrorLogEvent":
        return StructuredErrorLogEvent.deserialize(data)

    raise Exception(f"unknown event class {data['event_class']}")


class EventsSummary:
    """Provides summary of all events."""

    def __init__(self, output_dir, preload=False):
        """
        Initialize EventsSummary class

        Parameters
        ----------
        output_dir: str
            Path of jade output directory.
        preload: bool
            Load all events into memory; otherwise, load by name on demand.

        """
        self._events = defaultdict(list)
        self._output_dir = output_dir
        self._event_dir = os.path.join(output_dir, EVENT_DIR)
        os.makedirs(self._event_dir, exist_ok=True)
        self._job_outputs_dir = os.path.join(output_dir, JOBS_OUTPUT_DIR)
        event_files = os.listdir(self._event_dir)
        if not event_files:
            # The old "consolidate_events" code stored all events in one file
            # called events.json.  They are now stored in one file per event
            # type, but we still detect and convert the old format.  We can
            # remove this once we're sure the old format doesn't exist.
            legacy_file = os.path.join(output_dir, EVENTS_FILENAME)
            if os.path.exists(legacy_file):
                self._handle_legacy_file(legacy_file)
            else:
                self._consolidate_events()
                self._save_events_summary()
        elif preload:
            self._load_all_events()
        # else, events have already been consolidated, load them on demand

    def _most_recent_event_files(self):
        """
        Find most recent event log files in job outputs directory.

        Examples
        --------
        a/events.log
        a/events.log.1
        a/events.log.2

        b/events.log
        b/events.log.1
        b/events.log.2
        ...
        event_files = [a/events.log, b/events.log]

        Returns
        -------
        list, a list of event log files.
        """
        regex = re.compile(r"\w*events.log")
        return [
            os.path.join(self._output_dir, x)
            for x in os.listdir(self._output_dir)
            if regex.search(x)
        ]

    def _consolidate_events(self):
        """Find most recent event log files, and merge event data together."""
        for event_file in self._most_recent_event_files():
            with open(event_file, "r") as f:
                for line in f.readlines():
                    record = json.loads(line)
                    event = deserialize_event(record)
                    self._events[event.name].append(event)
        for name in self._events.keys():
            self._events[name].sort(key=lambda x: x.timestamp)

    def _deserialize_events(self, name, path):
        self._events[name] = [deserialize_event(x) for x in load_data(path)]

    def _get_events(self, name):
        if name not in self._events:
            self._load_event_file(name)

        return self._events.get(name, [])

    def _handle_legacy_file(self, legacy_file):
        with open(legacy_file) as f_in:
            for line in f_in:
                event = deserialize_event(json.loads(line.strip()))
                self._events[event.name].append(event)

        self._save_events_summary()
        os.remove(legacy_file)
        logger.info("Converted events to new format")

    def _load_all_events(self):
        for filename in os.listdir(self._event_dir):
            name = os.path.splitext(filename)[0]
            if name in self._events:
                continue
            path = os.path.join(self._event_dir, filename)
            self._deserialize_events(name, path)

    def _load_event_file(self, name):
        filename = self._make_event_filename(name)
        if os.path.exists(filename):
            self._deserialize_events(name, filename)

    def _make_event_filename(self, name):
        return os.path.join(self._event_dir, name) + ".json"

    def _save_events_summary(self):
        """Save events to one file per event name."""
        for name, events in self._events.items():
            dict_events = [event.to_dict() for event in events]
            dump_data(dict_events, self._make_event_filename(name))

    def get_bytes_consumed(self):
        """Return a sum of all bytes_consumed events.

        Returns
        -------
        int
            Size in bytes of files produced by all jobs

        """
        total = 0
        for event in self.iter_events(EVENT_NAME_BYTES_CONSUMED):
            total += event.data["bytes_consumed"]

        return total

    def get_config_exec_time(self):
        """Return the total number of seconds to run all jobs in the config.

        Returns
        -------
        int

        """
        events = self.list_events(EVENT_NAME_CONFIG_EXEC_SUMMARY)
        if not events:
            raise InvalidConfiguration("no batch summary events found")

        return events[0].data["config_execution_time"]

    def iter_events(self, name):
        """Return a generator over events with name.

        Parameters
        ----------
        name : str

        Yields
        ------
        event : StructuredLogEvent

        """
        for event in self._get_events(name):
            yield event

    def list_events(self, name):
        """Return the events of type name.

        Returns
        -------
        list
            list of StructuredLogEvent

        """
        return self._get_events(name)

    def list_unique_categories(self):
        """Return the unique event categories in the log. Will cause all events
        to get loaded into memory.

        Returns
        -------
        list

        """
        self._load_all_events()
        categories = set()
        for events in self._events.values():
            if not events:
                continue
            categories.add(events[0].category)

        categories = list(categories)
        categories.sort()
        return categories

    def list_unique_names(self):
        """Return the unique event names in the log.

        Returns
        -------
        list

        """
        return [os.path.splitext(x)[0] for x in os.listdir(self._event_dir)]

    def show_events(self, name):
        """Print tabular events in terminal"""
        table = PrettyTable()

        field_names = None
        count = 0
        for event in self.iter_events(name):
            if field_names is None:
                field_names = event.field_names()
            table.add_row(event.values())
            count += 1

        if count == 0:
            print(f"No events of type {name}")
            return

        table.field_names = field_names
        print(f"Events of type {name} from directory: {self._output_dir}")
        print(table)
        print(f"Total events: {count}\n")

    def show_events_in_category(self, category):
        """Print tabular events matching category in terminal. Will cause all
        events to get loaded into memory.

        """
        event_names = []
        self._load_all_events()
        for name, events in self._events.items():
            if not events:
                continue
            if events[0].category == category:
                event_names.append(name)

        if not event_names:
            print(f"There are no events in category {category}")
            return

        for event_name in sorted(event_names):
            self.show_events(event_name)

    def show_event_categories(self):
        """Show the unique event categories in the log."""
        print("Catgories:  {}".format(" ".join(self.list_unique_categories())))

    def show_event_names(self):
        """Show the unique event names in the log."""
        print("Names:  {}".format(" ".join(self.list_unique_names())))

    def to_json(self):
        """Return all events in JSON format.

        Returns
        -------
        str

        """
        self._load_all_events()
        return json.dumps(
            [x.to_dict() for events in self._events.values() for x in events], indent=2
        )
