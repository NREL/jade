"""
This model constains event object and event summary object.
"""
import json
import os
import sys
from datetime import datetime
from prettytable import PrettyTable
from jade.common import JOBS_OUTPUT_DIR
from jade.utils.utils import dump_data


class StructuredJobEvent(object):
    """
    A class for recording structured job event resulted in job failure.
    """
    def __init__(self, job_name, category, code, message, **kwargs):
        """
        Initialize the class

        Parameters
        ----------
        job_name: str,
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
        self.job_name = job_name
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

        :param output_dir: str, the path of jade output directory.
        """
        self._output_dir = output_dir
        self._job_outputs_dir = os.path.join(output_dir, JOBS_OUTPUT_DIR)
        self._summary_file = os.path.join(output_dir, "events.json")
        self._events = self._consolidate_events()

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
        event_files, job_outputs = [], [
            os.path.join(root, _file)
            for root, dirs, files in os.walk(self._job_outputs_dir)
            for _file in files
        ]
        for output in job_outputs:
            if not output.endswith("events.log"):
                continue

            event_files.append(output)

        return event_files

    def _consolidate_events(self):
        """Find most recent event log files, and merge event data together."""
        events = []
        for event_file in self._most_recent_event_files():
            with open(event_file, "r") as f:
                for line in f.readlines():
                    record = json.loads(line)
                    event = StructuredJobEvent(
                        job_name=record.get("job_name", ""),
                        category=record.get("category", ""),
                        code=record.get("code", ""),
                        message=record.get("message", ""),
                        timestamp=record.get("timestamp", ""),
                        exception=record.get("exception", ""),
                        **record["data"]
                    )
                    events.append(event)
        return events

    def _save_events_summary(self):
        """Save all events data to a JSON file"""
        dict_events = [event.to_dict() for event in self._events]
        dump_data(dict_events, self._summary_file)

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
                event.job_name,
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

        self._save_events_summary()
        print(f"Events summary file: {self._summary_file}")
