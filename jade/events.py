"""
This module contains StructuredEvent and EventSummary classes.
"""

import json
import os
import re
import sys
from datetime import datetime

from prettytable import PrettyTable

from jade.common import JOBS_OUTPUT_DIR
from jade.utils.utils import dump_data, get_filenames_in_path, load_data


EVENT_CATEGORY_HPC = "HPC"
EVENT_CATEGORY_RESOURCE_UTIL = "ResourceUtilization"

EVENT_CODE_HPC_SUBMIT = "100"
EVENT_CODE_HPC_JOB_ASSIGNED = "101"
EVENT_CODE_HPC_JOB_STATE_CHANGE = "102"
EVENT_CODE_CPU_STATS = "103"
EVENT_CODE_DISK_STATS = "104"
EVENT_CODE_MEMORY_STATS = "105"
EVENT_CODE_NETWORK_STATS = "106"


class StructuredEvent(object):
    """
    A class for recording structured log events.
    """
    def __init__(self, name, category, code, message, **kwargs):
        """
        Initialize the class

        Parameters
        ----------
        name: str,
            the key of a job.
        category: str,
            An event category given by the user.
        code: str,
            An event code given by the user.
        message:
            An event message given the user.

        kwargs:
            Other information that the user needs to record into event.
        """
        self.name = name
        self.category = category
        self.code = code
        self.message = message

        if "timestamp" in kwargs:
            self.timestamp = kwargs.pop("timestamp")
        else:
            self.timestamp = str(datetime.now())

        if "exception" in kwargs:
            self.exception = kwargs.pop("exception")

        self.data = kwargs

    @classmethod
    def deserialize(cls, record):
        """Deserialize event from JSON.

        Parameters
        ----------
        record : dict

        Returns
        -------
        StructuredEvent

        """
        return cls(
            name=record.get("name", ""),
            category=record.get("category", ""),
            code=record.get("code", ""),
            message=record.get("message", ""),
            timestamp=record.get("timestamp", ""),
            exception=record.get("exception", ""),
            **record["data"]
        )

    def parse_traceback(self):
        """
        Parse the system exception information - exception, filename, and lineno.
        """
        exc_type, exc_obj, tb = sys.exc_info()

        if tb is None:
            self.exception = ""
            return

        error = str(exc_obj).strip()
        filename = os.path.basename(tb.tb_frame.f_code.co_filename)
        lineno = tb.tb_lineno

        exception = "File: {}, Line: {}, Error: {}".format(filename, lineno, error)
        self.exception = exception

    def __str__(self):
        """To format a event instance to string"""
        return json.dumps(self.__dict__, sort_keys=True)

    def to_dict(self):
        """Convert event object to dict"""
        return self.__dict__


class EventsSummary(object):
    """Provides summary of failed job events or errors."""

    def __init__(self, output_dir):
        """
        Initialize EventsSummary class

        Parameters
        ----------
        output_dir: str
            Path of jade output directory.

        """
        self._output_dir = output_dir
        self._job_outputs_dir = os.path.join(output_dir, JOBS_OUTPUT_DIR)
        self._summary_file = os.path.join(output_dir, "events.json")
        if os.path.exists(self._summary_file):
            self._events = [
                StructuredEvent.deserialize(x) for x in load_data(self._summary_file)
            ]
        else:
            self._events = self._consolidate_events()
            self._save_events_summary()

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
        return get_filenames_in_path(self._output_dir, regex, is_regex=True)

    def _consolidate_events(self):
        """Find most recent event log files, and merge event data together."""
        events = []
        for event_file in self._most_recent_event_files():
            with open(event_file, "r") as f:
                for line in f.readlines():
                    record = json.loads(line)
                    event = StructuredEvent.deserialize(record)
                    events.append(event)
        events.sort(key=lambda x: x.timestamp)
        return events

    def _save_events_summary(self):
        """Save all events data to a JSON file"""
        dict_events = [event.to_dict() for event in self._events]
        dump_data(dict_events, self._summary_file)

    @property
    def events(self):
        """Return the events.

        Returns
        -------
        list
            list of StructuredEvent

        """
        return self._events

    def show_events(self):
        """Print tabular events in terminal"""
        print(f"Events from directory: {self._output_dir}")
        table = PrettyTable()
        table.field_names = [
            "Job Name", "Timestamp", "Category", "Code",
            "Message", "Data", "Exception"
        ]
        for event in self._events:
            table.add_row([
                event.name,
                event.timestamp,
                event.category,
                event.code,
                event.message,
                event.data,
                event.exception
            ])
        total = len(self._events)
        print(table)
        print(f"Total events: {total}\n")
