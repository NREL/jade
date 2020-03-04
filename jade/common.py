"""Common definitions for the jade package."""

import os


OUTPUT_DIR = "output"
JOBS_OUTPUT_DIR = "job-outputs"
SCRIPTS_DIR = "scripts"
CONFIG_FILE = "config.json"
RESULTS_DIR = "temp-results"
RESULTS_FILE = "results.json"
ANALYSIS_DIR = "analysis"
POST_PROCESSING_CONFIG_FILE = "post-config.json"


def get_results_temp_filename(output_dir, batch_id):
    """Get the results temp filename for a batch of jobs.

    Parameters
    ----------
    output_dir : str
        output directory for all jobs
    batch_id : int
        batch ID of jobs running on a node

    Returns
    -------
    str

    """
    # This uses CSV files because it allows for cheap appends by jobs.
    # For JSON and TOML each job would have to parse all existing jobs before
    # appending.
    return os.path.join(
        output_dir, RESULTS_DIR, f"results_batch_{batch_id}.csv"
    )
