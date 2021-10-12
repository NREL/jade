"""Synchronizes updates to the results file across jobs."""

import csv
import glob
import logging
import os
import time
from pathlib import Path

from filelock import SoftFileLock, Timeout

from jade.common import RESULTS_DIR
from jade.result import Result, deserialize_result, serialize_result


LOCK_TIMEOUT = 300
PROCESSED_RESULTS_FILENAME = "processed_results.csv"

logger = logging.getLogger(__name__)


class ResultsAggregator:
    """Synchronizes updates to the results file.

    One instance is used to aggregate results from all compute nodes.
    One instance is used for each compute node.
    """

    def __init__(self, filename, timeout=LOCK_TIMEOUT, delimiter=","):
        """
        Constructs ResultsAggregator.

        Parameters
        ----------
        filename : Path
            Results file.
        timeout : int
            Lock acquistion timeout in seconds.
        delimiter : str
            Delimiter to use for CSV formatting.

        """
        self._filename = filename
        self._lock_file = self._filename.parent / (self._filename.name + ".lock")
        self._timeout = timeout
        self._delimiter = delimiter
        self._is_node = "batch" in filename.name

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
        agg = cls(Path(output_dir) / PROCESSED_RESULTS_FILENAME, **kwargs)
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
        return cls(Path(output_dir) / PROCESSED_RESULTS_FILENAME, **kwargs)

    @classmethod
    def load_node_results(cls, output_dir, batch_id, **kwargs):
        """Load a per-node instance from an output directory.

        Parameters
        ----------
        output_dir : str
        batch_id : int

        Returns
        -------
        ResultsAggregator

        """
        return cls(Path(output_dir) / RESULTS_DIR / f"results_batch_{batch_id}.csv", **kwargs)

    @classmethod
    def load_node_results_file(cls, path, **kwargs):
        """Load a per-node instance from an output directory.

        Parameters
        ----------
        path : Path

        Returns
        -------
        ResultsAggregator

        """
        return cls(path, **kwargs)

    @staticmethod
    def _get_fields():
        return Result._fields

    def _do_action_under_lock(self, func, *args, **kwargs):
        # Using this instead of FileLock because it will be used across nodes
        # on the Lustre filesystem.
        lock = SoftFileLock(self._lock_file, timeout=self._timeout)
        start = time.time()
        try:
            lock.acquire(timeout=self._timeout)
        except Timeout:
            # Picked a default value such that this should not trip. If it does
            # trip under normal circumstances then we need to reconsider this.
            logger.error(
                "Failed to acquire file lock %s within %s seconds", self._lock_file, self._timeout
            )
            raise

        duration = time.time() - start
        if duration > 10:
            logger.warning("Acquiring ResultsAggregator lock took too long: %s", duration)

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
        with open(self._filename, "w") as f_out:
            f_out.write(self._delimiter.join(self._get_fields()))
            f_out.write("\n")

    @classmethod
    def append(cls, output_dir, result, batch_id=None):
        """Append a result to the file.

        output_dir : str
        result : Result
        batch_id : int

        """
        if batch_id is None:
            aggregator = cls.load(output_dir)
        else:
            aggregator = cls.load_node_results(output_dir, batch_id)
        aggregator.append_result(result)

    def append_result(self, result):
        """Append a result to the file.

        result : Result

        """
        start = time.time()
        text = self._delimiter.join([str(getattr(result, x)) for x in self._get_fields()])
        self._do_action_under_lock(self._append_result, text)
        duration = time.time() - start
        if duration > 10:
            logger.warning("Appending a result took too long: %s", duration)

    def _append_result(self, text):
        with open(self._filename, "a") as f_out:
            if f_out.tell() == 0:
                f_out.write(self._delimiter.join(self._get_fields()))
                f_out.write("\n")
            f_out.write(text)
            f_out.write("\n")

    def _append_processed_results(self, results):
        assert not self._is_node
        with open(self._filename, "a") as f_out:
            for result in results:
                text = self._delimiter.join([str(getattr(result, x)) for x in self._get_fields()])
                f_out.write(text)
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
        with open(self._filename, "w") as f_out:
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
        unprocessed_results = list((self._filename / RESULTS_DIR).glob("results*.csv"))
        if unprocessed_results:
            logger.error("Found unprocessed results: %s", unprocessed_results)
        # TODO: Older code included unprocessed results here. Not sure why.
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

    def move_results(self, func):
        """Move the results to a new location and delete the file.

        Parameters
        ----------
        func : function

        Returns
        -------
        list
            list of Result

        """
        return self._do_action_under_lock(self._move_results, func)

    def _move_results(self, func):
        results = self._get_results()
        func(results)
        os.remove(self._filename)
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
        assert not self._is_node
        return self._do_action_under_lock(self._process_results)

    def _get_node_results_files(self):
        assert not self._is_node
        return list((self._filename.parent / RESULTS_DIR).glob("results_batch_*.csv"))

    def _process_results(self):
        results = []
        for path in self._get_node_results_files():
            agg = ResultsAggregator.load_node_results_file(path)
            results += agg.move_results(self._append_processed_results)
        return results
