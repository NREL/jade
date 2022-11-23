"""Postprocesses Jade results"""

import os
import shutil
from pathlib import Path

from jade.jobs.job_configuration_factory import create_config_from_file
from jade.result import ResultsSummary

from ingest_data import create_table, ingest_data


def main():
    output_dir = Path(os.environ["JADE_RUNTIME_OUTPUT"])
    config_file = output_dir / "config.json"
    config = create_config_from_file(config_file)

    user_data = {}
    for key in config.list_user_data_keys():
        val = config.get_user_data(key)
        user_data[key] = val
        print(f"config user data key={key} = {val}")

    job_results = []
    summary = ResultsSummary(Path(os.environ["JADE_RUNTIME_OUTPUT"]))
    with open(output_dir / "simulation_postprocess_results.txt", "w") as f_out:
        for result in summary.results:
            if result.return_code == 0:
                job = config.get_job(result.name)
                f_out.write(f"Found successful job name={result.name}\n")
                print(f"job name = {job.name} has ext = {job.ext}")
                job_results.append(
                    {
                        "name": job.name,
                        "path": output_dir / "job-outputs" / job.name,  # one level deeper?
                        "timestamp": result.completion_time,
                        "exec_time_s": result.exec_time_s,
                        # Next three should come from user_data and ext or by processing the
                        # output data.
                        "scenario": "my scenario",
                        "system_name": "my system name",
                        "tags": "my tags",
                        # TODO: what about job stdio and stderr? Should we copy the files to <path>?
                    }
                )
            else:
                f_out.write(
                    f"Found failed job name={result.name} return_code={result.return_code}\n"
                )

        missing_jobs = summary.get_missing_jobs(config.iter_jobs())
        if missing_jobs:
            f_out.write(f"Found {len(missing_jobs)} missing jobs\n")

    data_path = Path("my_test_results")
    if data_path.exists():
        shutil.rmtree(data_path)
    data_path.mkdir()
    db_file = data_path / "my_results.db"
    create_table(db_file)
    ingest_data(job_results, db_file, data_path, max_workers=None)


if __name__ == "__main__":
    main()
