"""Synchronizes updates to the results file across jobs."""

import csv
import glob
import logging
import os

from filelock import SoftFileLock, Timeout

from jade.common import get_results_filename, get_temp_results_filename
from jade.result import Result, deserialize_result, serialize_result


logger = logging.getLogger(__name__)


class ResultsAggregator:
    """Synchronizes updates to the results file across jobs on one system."""

    def __init__(self, path, timeout=60, delimiter=","):
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
        self._filename = get_temp_results_filename(path)
        self._processed_filename = get_results_filename(path)
        self._lock_file = self._filename + ".lock"
        self._timeout = timeout
        self._delimiter = delimiter

    @classmethod
    def create(cls, output_dir, **kwargs):
        """Create a new instance.

        Parameters
        ----------
        output_dir : str

        Returns
        -------
        ResultsAggregator

        """
        agg = cls(output_dir, **kwargs)
        agg.create_files()
        return agg

    @classmethod
    def load(cls, output_dir, **kwargs):
        """Load an instance from an output directory.

        Parameters
        ----------
        output_dir : str

        Returns
        -------
        ResultsAggregator

        """
        return cls(output_dir, **kwargs)

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
                "Failed to acquire file lock %s within %s seconds", self._lock_file, self._timeout
            )
            raise

        try:
            return func(*args, **kwargs)
        finally:
            lock.release()

    def create_files(self):
        """Initialize the results file. Should only be called by the parent
        process.

        """
        self._do_action_under_lock(self._create_files)

    def _create_files(self):
        for filename in (self._filename, self._processed_filename):
            with open(filename, "w") as f_out:
                f_out.write(self._delimiter.join(self._get_fields()))
                f_out.write("\n")

    @classmethod
    def append(cls, output_dir, result):
        """Append a result to the file.

        result : Result

        """
        aggregator = cls.load(output_dir)
        aggregator.append_result(result)

    def append_result(self, result):
        """Append a result to the file.

        result : Result

        """
        text = self._delimiter.join([str(getattr(result, x)) for x in self._get_fields()])
        self._do_action_under_lock(self._append_result, text)

    def _append_result(self, text):
        with open(self._filename, "a") as f_out:
            f_out.write(text)
            f_out.write("\n")

    def _append_processed_results(self, results):
        with open(self._processed_filename, "a") as f_out:
            for result in results:
                text = self._delimiter.join([str(getattr(result, x)) for x in self._get_fields()])
                f_out.write(text)
                f_out.write("\n")

    def _clear_temp_results(self):
        """Clear the file, once the results have been processed."""
        with open(self._filename, "w") as f_out:
            f_out.write(self._delimiter.join(self._get_fields()))
            f_out.write("\n")

    def clear_results_for_resubmission(self, jobs_to_resubmit):
        """Remove jobs that will be resubmitted from the results file.

        Parameters
        ----------
        jobs_to_resubmit : set
            Job names that will be resubmitted.

        """
        results = [x for x in self.get_results() if x.name not in jobs_to_resubmit]
        self._write_results(results)
        logger.info("Cleared %s results from %s", len(results), self._filename)

    def clear_unsuccessful_results(self):
        """Remove failed and canceled results from the results file."""
        results = [x for x in self.get_results() if x.return_code == 0]
        self._write_results(results)
        logger.info("Cleared failed results from %s", self._filename)

    def _write_results(self, results):
        _results = [serialize_result(x) for x in results]
        with open(self._processed_filename, "w") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=_results[0].keys())
            writer.writeheader()
            if results:
                writer.writerows(_results)

    def get_results(self):
        """Return the current results.

        Returns
        -------
        list
            list of Result objects

        """
        return self._do_action_under_lock(self._get_all_results)

    def get_results_unsafe(self):
        """Return the results. It is up to the caller to ensure that
        a lock is not needed.

        Returns
        -------
        list
            list of Result objects

        """
        return self._get_results()

    def _get_all_results(self):
        # Include unprocessed and processed results.
        return self._get_results(processed_results=True) + self._get_results(
            processed_results=False
        )

    def _get_results(self, processed_results=True):
        filename = self._processed_filename if processed_results else self._filename
        with open(filename) as f_in:
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
        results = cls.load(output_dir, **kwargs)
        return results.get_results()

    def process_results(self):
        """Move all temp results into the consolidated file, then clear the file.

        Returns
        -------
        list
            list of Result objects that are newly completed

        """
        return self._do_action_under_lock(self._process_results)

    def _process_results(self):
        results = self._get_results(processed_results=False)
        self._append_processed_results(results)
        self._clear_temp_results()
        return results
