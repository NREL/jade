"""Common definitions for the jade package."""

import os


OUTPUT_DIR = "output"
JOBS_OUTPUT_DIR = "job-outputs"
SCRIPTS_DIR = "scripts"
CONFIG_FILE = "config.json"
RESULTS_FILE = "results.json"
ANALYSIS_DIR = "analysis"
POST_PROCESSING_CONFIG_FILE = "post-config.json"
HPC_CONFIG_FILE = "hpc_config.toml"
DEFAULT_SUBMISSION_GROUP = "default"


def get_results_filename(output_dir):
    """Get the temporary results filename for all jobs.

    Parameters
    ----------
    output_dir : str
        output directory for all jobs

    Returns
    -------
    str

    """
    # This uses a CSV file because it allows for cheap appends by jobs.
    # For JSON and TOML each job would have to parse all existing jobs before
    # appending.
    return os.path.join(output_dir, "processed_results.csv")


def get_temp_results_filename(output_dir):
    """Get the temporary results filename for all jobs.

    Parameters
    ----------
    output_dir : str
        output directory for all jobs

    Returns
    -------
    str

    """
    return os.path.join(output_dir, "results.csv")
