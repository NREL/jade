"""Defines a Result object."""

from collections import namedtuple
import os
import time

from prettytable import PrettyTable

from jade.common import RESULTS_FILE
from jade.exceptions import InvalidParameter, ExecutionError
from jade.utils.utils import load_data

class Result(namedtuple("Result", "name, return_code, status, exec_time_s, completion_time")):
    """
    Result class containing data after jobs have finished. `completion_time`
    intended to be populated when result created, then reused as needed.

    Attributes
    ----------
    name : str
    return_code : int
    status : str
    exec_time_s : int
    completion_time : int (default current timestamp)

    """
    def __new__(cls, name, return_code, status, exec_time_s,
                completion_time=time.time()):
        # add default values
        return super(Result, cls).__new__(cls, name, return_code, status,
                                          exec_time_s, completion_time)

def serialize_result(result):
    """Serialize a Result to a dict.

    Parameters
    ----------
    result : Result

    Returns
    -------
    dict

    """
    data = result._asdict()
    return data


def serialize_results(results):
    """Serialize a list of Result objects.

    Parameters
    ----------
    result : Result

    Returns
    -------
    list of dict

    """
    return [serialize_result(x) for x in results]


def deserialize_result(data):
    """Deserialize a Result from raw data.

    Parameters
    ----------
    data : dict

    Returns
    -------
    Result

    """
    if "completion_time" in data.keys():
        return Result(data["name"], data["return_code"], data["status"],
                  data["exec_time_s"], data["completion_time"])
    else:
        return Result(data["name"], data["return_code"], data["status"],
                  data["exec_time_s"])


def deserialize_results(data):
    """Deserialize a list of Result objects from raw data.

    Parameters
    ----------
    data : list of dict

    Returns
    -------
    list of Result

    """
    return [deserialize_result(x) for x in data]


class ResultsSummary:
    """Provides summary of all job results."""
    def __init__(self, output_dir):
        self._output_dir = output_dir

        self._results_file = os.path.join(output_dir, RESULTS_FILE)
        data = self._parse(self._results_file)
        data["results"] = deserialize_results(data["results"])
        self._results = data
        self._base_directory = data["base_directory"]

    @property
    def base_directory(self):
        """Return the base directory for the job results.

        Returns
        -------
        str

        """
        return self._base_directory

    @property
    def results(self):
        """Return the results.

        Returns
        -------
        list
            list of Result objects

        """
        return self._results

    @staticmethod
    def _parse(results_file):
        return load_data(results_file)

    def get_successful_result(self, job_name):
        """Return the job result from the results file.

        Parameters
        ----------
        results_file : str
        job_name : str

        Returns
        -------
        dict

        Raises
        ------
        InvalidParameter
            Raised if job_name is not found.
        ExecutionError
            Raised if the result was not successful.

        """
        for result in self._results["results"]:
            if job_name == result.name:
                if result.return_code != 0 or result.status != "finished":
                    raise ExecutionError(f"result wasn't successful: {result}")

                return result

        raise InvalidParameter(f"result not found {job_name}")

    def list_results(self):
        """Return the results.

        Returns
        -------
        list

        """
        return self._results["results"][:]

    def show_results(self, only_failed=False, only_successful=False):
        """Show the results in a table."""
        if only_successful and only_failed:
            raise InvalidParameter(
                "only_failed and only_successful are mutually exclusive"
            )

        print(f"Results from directory: {self._output_dir}")
        print(f"JADE Version: {self._results['jade_version']}")
        print(f"{self._results['timestamp']}\n")

        #if "repository_info" in self._results:
        #    git_status = self._results["repository_info"]["status"]
        #    print(f"git status:  {git_status}")

        num_successful = 0
        num_failed = 0
        table = PrettyTable()
        table.field_names = ["Job Name", "Return Code", "Status",
                             "Execution Time (s)", "Completion Time"]
        min_exec = 0
        max_exec = 0
        if self._results["results"]:
            min_exec = self._results["results"][0].exec_time_s
            max_exec = self._results["results"][0].exec_time_s

        exec_times = []
        for result in self._results["results"]:
            if result.return_code == 0 and result.status == "finished":
                num_successful += 1
            else:
                num_failed += 1
            if only_failed and result.return_code == 0:
                continue
            if only_successful and result.return_code == 1:
                continue
            if result.exec_time_s < min_exec:
                min_exec = result.exec_time_s
            if result.exec_time_s > max_exec:
                max_exec = result.exec_time_s
            exec_times.append(result.exec_time_s)
            table.add_row([result.name, result.return_code, result.status,
                           result.exec_time_s, result.completion_time])

        total = num_successful + num_failed
        assert total == len(self._results["results"])
        avg_exec = sum(exec_times) / len(exec_times)

        print(table)
        print(f"\nNum successful: {num_successful}")
        print(f"Num failed: {num_failed}")
        print(f"Total: {total}\n")
        print("Avg execution time (s): {:.2f}".format(avg_exec))
        print("Min execution time (s): {:.2f}".format(min_exec))
        print("Max execution time (s): {:.2f}\n".format(max_exec))
