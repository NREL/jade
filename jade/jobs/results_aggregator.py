"""Synchronizes updates to the results file across jobs."""

import csv
import logging
import os

from filelock import FileLock, Timeout

from jade.result import Result, deserialize_result


logger = logging.getLogger(__name__)


class ResultsAggregator:
    """Synchronizes updates to the results file across jobs on one system.
    To use on different systems then the code must use SoftFileLock instead
    of FileLock.

    """
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

    @staticmethod
    def _get_fields():
        return Result._fields

    def _do_action_under_lock(self, func, *args, **kwargs):
        lock = FileLock(self._lock_file, timeout=self._timeout)
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
        self._do_action_under_lock(self._append_result, result)
        self._add_completion(result)

    def _add_completion(self, result):
        completion_filename = os.path.join(
            os.path.dirname(self._filename),
            result.name,
        )
        with open(completion_filename, "w") as _:
            pass

    def _append_result(self, result):
        text = self._delimiter.join(
            [str(getattr(result, x)) for x in self._get_fields()]
        )

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


class ResultsAggregatorSummary:
    """Summarizes all ResultsAggregator instances."""
    def __init__(self, path):
        self._path = path
        self._aggregators = []
        self._completed_jobs = set()

    @property
    def completed_jobs(self):
        """Return the completed jobs.

        Returns
        -------
        set

        """
        return self._completed_jobs

    def delete_files(self):
        """Delete results files from all ResultsAggregator instances."""
        for aggregator in self._aggregators:
            aggregator.delete_file()

    def get_results(self):
        """Return results from all ResultsAggregator instances.
        This assumes that all CSV files in stored in path are from
        ResultsAggregators.

        """
        self._aggregators[:] = []
        results = []

        for filename in os.listdir(self._path):
            if os.path.splitext(filename)[1] == ".csv":
                results_file = os.path.join(self._path, filename)
                aggregator = ResultsAggregator(results_file)
                results += aggregator.get_results()
                self._aggregators.append(aggregator)

        return results

    def update_completed_jobs(self):
        """Check for completed jobs."""
        for filename in os.listdir(self._path):
            if not filename.endswith(".csv"):
                logger.debug("Detected completion of job=%s", filename)
                self._completed_jobs.add(filename)
                os.remove(os.path.join(self._path, filename))
