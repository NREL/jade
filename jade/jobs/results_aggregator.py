"""Synchronizes updates to the results file across jobs."""

import csv
import glob
import logging
import os

from filelock import SoftFileLock, Timeout

from jade.common import get_results_filename, RESULTS_DIR
from jade.result import Result, deserialize_result


logger = logging.getLogger(__name__)


class ResultsAggregator:
    """Synchronizes updates to the results file across jobs on one system."""
    def __init__(self, filename, timeout=30, delimiter=","):
        """
        Constructs ResultsAggregator.

        Parameters
        ----------
        filename : str
            Full path to results filename. Must be accessible by all workers.
        timeout : int
            Lock acquistion timeout in seconds.
        delimiter : str
            Delimiter to use for CSV formatting.

        """
        self._filename = filename
        self._lock_file = self._filename + ".lock"
        self._timeout = timeout
        self._delimiter = delimiter
        self._temp_results_dir = os.path.join(
            os.path.dirname(filename),
            RESULTS_DIR
        )

    @staticmethod
    def _get_fields():
        return Result._fields

    def _do_action_under_lock(self, func, *args, **kwargs):
        # Using this instead of FileLock because it will be used across nodes
        # on the Lustre filesystem.
        lock = SoftFileLock(self._lock_file, timeout=self._timeout)
        try:
            lock.acquire(timeout=self._timeout)
        except Timeout:
            # Picked a default value such that this should not trip. If it does
            # trip under normal circumstances then we need to reconsider this.
            logger.error(
                "Failed to acquire file lock %s within %s seconds",
                self._lock_file, self._timeout
            )
            raise

        try:
            return func(*args, **kwargs)
        finally:
            lock.release()

    def create_file(self):
        """Initialize the results file. Should only be called by the parent
        process.

        """
        self._do_action_under_lock(self._create_file)

    def _create_file(self):
        with open(self._filename, "w") as f_out:
            f_out.write(self._delimiter.join(self._get_fields()) + "\n")

    def delete_file(self):
        """Delete the results file and lock file."""
        assert os.path.exists(self._filename)
        os.remove(self._filename)
        if os.path.exists(self._lock_file):
            os.remove(self._lock_file)
        logger.debug("Deleted results file %s", self._filename)

    @classmethod
    def append(cls, filename, result):
        """Append a result to the file.

        result : Result

        """
        aggregator = cls(filename)
        aggregator.append_result(result)

    def append_result(self, result):
        """Append a result to the file.

        result : Result

        """
        text = self._delimiter.join(
            [str(getattr(result, x)) for x in self._get_fields()]
        )
        self._do_action_under_lock(self._append_result, text)
        self._add_completion(result)

    def _add_completion(self, result):
        completion_filename = os.path.join(
            self._temp_results_dir,
            result.name,
        )
        with open(completion_filename, "w") as _:
            pass

    def _append_result(self, text):
        with open(self._filename, "a") as f_out:
            f_out.write(text + "\n")

    def get_results(self):
        """Return the current results.

        Returns
        -------
        list
            list of Result objects

        """
        return self._do_action_under_lock(self._get_results)

    def get_results_unsafe(self):
        """Return the results. It is up to the caller to ensure that
        a lock is not needed.

        Returns
        -------
        list
            list of Result objects

        """
        return self._get_results()

    def _get_results(self):
        with open(self._filename) as f_in:
            results = []
            reader = csv.DictReader(f_in, delimiter=self._delimiter)
            for row in reader:
                row["return_code"] = int(row["return_code"])
                row["exec_time_s"] = float(row["exec_time_s"])
                row["completion_time"] = float(row["completion_time"])
                result = deserialize_result(row)
                results.append(result)

            return results

    @classmethod
    def list_results(cls, output_dir, **kwargs):
        """Return the current results.

        Parameters
        ----------
        output_dir : str

        Returns
        -------
        list
            list of Result objects

        """
        results_file = get_results_filename(output_dir)
        results = cls(results_file, **kwargs)
        return results.get_results()
