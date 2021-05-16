"""
The job execution class and methods for auto-regression
"""
import os

import numpy as np
import pandas as pd
from statsmodels.tsa.ar_model import AR
import matplotlib

from jade.events import StructuredErrorLogEvent, EVENT_CATEGORY_ERROR, EVENT_NAME_UNHANDLED_ERROR
from jade.jobs.job_execution_interface import JobExecutionInterface
from jade.loggers import log_event
from jade.utils.utils import dump_data


def autoregression_analysis(country, data, output):
    """
    Country based GDP auto-regression analysis

    Parameters
    ----------
    country: str
        the name of a country
    data: str
        path to the csv file containing the GDP data.
    output: str
        The path to the output directory

    Returns
    -------
    tuple, The path of csv result file, and the path of png plot file.
    """
    # Read csv
    df = pd.read_csv(data, index_col="year")
    df = df.dropna()

    # Train model
    train = df["gdp"].values
    model = AR(train)
    model_fit = model.fit()

    # Validate model
    lag = model_fit.k_ar
    pred = model_fit.predict(start=lag, end=len(train), dynamic=False)

    # Save result
    df["pred_gdp"] = [np.nan for _ in range(lag - 1)] + list(pred)
    result_file = os.path.join(output, "result.csv")
    df.to_csv(result_file)

    # Save plot
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    df.plot()
    plt.grid(axis="y", linestyle="--")
    plt.title(country + "(current $)")
    plot_file = os.path.join(output, "result.png")
    plt.savefig(plot_file)

    return result_file, plot_file


class AutoRegressionExecution(JobExecutionInterface):
    """
    A class used for auto-regression job execution on computer.
    """

    def __init__(self, job, output, output_format="csv"):
        """
        Init auto-regression execution class

        Parameters
        ----------
        job: :obj:`AutoRegressionParameters`
            The instance of :obj:`AutoRegressionParameters`
        output: str,
            The path to the output directory.
        output_format: str,
            The export format of result file, default 'csv'.
        """
        self._job = job
        self._output = output
        self._output_format = output_format
        self._job_dir = os.path.join(output, job.name)
        os.makedirs(self._job_dir, exist_ok=True)

    @property
    def results_directory(self):
        """Return the results directory created by the simulation."""
        return self._output

    @classmethod
    def create(cls, _, job, output, **kwargs):
        """Create instance of :obj:`AutoRegressionExecution`"""
        return cls(job, output)

    @staticmethod
    def generate_command(job, output, config_file, verbose=False):
        """
        Generate command consumed by bash for running auto-regression analysis.

        Parameters
        ----------
        job: :obj:`AutoRegressionParameters`
            The instance of :obj:`AutoRegressionParameters`.
        output: str
            The path to the output directory.
        config_file: str,
            The path to the configuration json file of job inputs.
        verbose: bool
            True if verbose, otherwise False.

        Returns
        -------
            str, A command line string
        """
        extension = "demo"
        command = [
            f"jade-internal run {extension}",
            f"--name={job.name}",
            f"--output={output}",
            f"--config-file={config_file}",
        ]

        if verbose:
            command.append("--verbose")

        return " ".join(command)

    def list_results_files(self):
        """Return a list of result filenames created by the simulation."""
        return [os.path.join(self._output, x) for x in os.listdir(self._output)]

    def post_process(self, **kwargs):
        """Run post-process operations on data."""

    def run(self):
        """Runs the autoregression, and return status code"""
        try:
            result_file, plot_file = autoregression_analysis(
                country=self._job.country, data=self._job.data, output=self._job_dir
            )
            summary_data = {
                "name": self._job.name,
                "country": self._job.country,
                "output": self._output,
                "result": result_file,
                "plot": plot_file,
            }
            summary_file = os.path.join(self._job_dir, "summary.toml")
            dump_data(summary_data, summary_file)
            if self._job.country == "australia":
                raise Exception("test")

        # Log event into file
        except Exception:
            # Create event instance
            event = StructuredErrorLogEvent(
                source=self._job.name,
                category=EVENT_CATEGORY_ERROR,
                name=EVENT_NAME_UNHANDLED_ERROR,
                message="Analysis failed!",
            )

            # Log event into file with structured message.
            log_event(event)

            # Must raise the exception here, or job returncode is 0 even it fails.
            raise

        return 0
