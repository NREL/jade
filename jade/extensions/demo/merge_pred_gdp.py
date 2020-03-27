"""This is a batch post-process script.

Merge predicted GDP of all countries into one CSV file.
"""
import logging
import os

import click
import pandas as pd


logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
@click.argument("output")
def run(output):
    """Merge the pred_gdp in CSV result into one CSV file."""
    logger.info("Start batch post-processing...")
    
    # Get the job-outputs directory
    job_outputs = os.path.join(output, "job-outputs")
    
    # Merge pred_gdp columns
    results = pd.DataFrame()
    for job_name in os.listdir(job_outputs):
        csv_result = os.path.join(job_outputs, job_name, "result.csv")
        if not os.path.exists(csv_result):
            continue
        df = pd.read_csv(csv_result)
        if results.empty: 
            results["year"] = df["year"]
        results[job_name] = df["pred_gdp"]
    
    # Export merged results to CSV file
    batch_post_process_dir = os.path.join(output, "batch-post-process")
    os.makedirs(batch_post_process_dir, exist_ok=True)
    csv_file = os.path.join(batch_post_process_dir, "pred_gdp.csv")
    results.to_csv(csv_file, index=False)
    
    logger.info("Batch post-processing finished!")


if __name__ == "__main__":
    cli()
