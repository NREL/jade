"""Ingests data from job runs into a sqlite database."""

import copy
import getpass
import itertools
import json
import shutil
import sqlite3
import tempfile
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path


TABLE_NAME = "job"


def create_table(db_file):
    """Create a table in the database."""
    con = sqlite3.connect(db_file)
    cur = con.cursor()
    cur.execute(
        f"""
        CREATE TABLE {TABLE_NAME}(
            id TEXT PRIMARY KEY
            ,user TEXT NOT NULL
            ,name TEXT NOT NULL
            ,timestamp TEXT NOT NULL
            ,exec_time_s TEXT NOT NULL
            ,path TEXT NOT NULL
            ,scenario TEXT NOT NULL
            ,system_name TEXT NOT NULL
            ,tags TEXT NOT NULL
        )
    """
    )
    con.commit()


def ingest_data(results, db_file, data_dst, copy_files=True, user=None, max_workers=1):
    """Ingest data from multiple jobs into the database.

    Parameters
    ----------
    results : list[dict]
        List of job results
    db_file : Path
        SQLite database file; must already exist with the table defined
    data_dst : Path
        Destination location to record metadata and optionally copy data.
    copy_files : bool
        If True, copy source files to data_dst. Defaults to True.
    user : str | None
        Record database update with this user name. If None, use the current user.
    max_workers : int | None
        Number of worker processes to use for copying data. If None, use all CPUs. Defaults to 1.
    """
    if not results:
        raise Exception("results is empty")

    params = {"data_dst": data_dst, "copy_files": copy_files, "user": user}
    data = []
    if max_workers == 1:
        data += [_process_job_result(x, params) for x in results]
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for result in executor.map(_process_job_result, results, itertools.repeat(params)):
                data.append(result)

    con = sqlite3.connect(db_file)
    cur = con.cursor()
    placeholder = ""
    num_columns = len(data[0])
    for i in range(num_columns):
        if i == num_columns - 1:
            placeholder += "?"
        else:
            placeholder += "?, "
    query = f"INSERT INTO {TABLE_NAME} VALUES({placeholder})"
    cur.executemany(query, data)
    con.commit()


def _process_job_result(result, params):
    copy_files = params["copy_files"]
    user = params["user"]
    res = copy.deepcopy(result)
    res["id"] = str(uuid.uuid4())
    dst = params["data_dst"] / res["id"]
    dst.mkdir()
    src = res["path"]
    res["path"] = str(dst)
    res["user"] = user or getpass.getuser()
    res["timestamp"] = str(datetime.fromtimestamp(res["timestamp"]))
    if copy_files:
        shutil.copytree(src, dst / "data")
        print(f"Copied data for job {res['name']} from {src} to {dst}")
    metadata_file = dst / "metadata.json"
    metadata_file.write_text(json.dumps(res, indent=2))
    print(f"Recorded metadata for job {res['name']} to {metadata_file}")
    return (
        res["id"],
        res["user"],
        res["name"],
        res["timestamp"],
        res["exec_time_s"],
        res["path"],
        res["scenario"],
        res["system_name"],
        res["tags"],
    )


def _run_tests():
    with tempfile.NamedTemporaryFile() as f:
        db_file = Path(f.name)
        create_table(db_file)
        results = [
            {
                "name": "job1",
                "path": "some_path",
                "timestamp": datetime.strptime(
                    "2022-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
                ).timestamp(),
                "exec_time_s": "3600",
                "scenario": "my scenario",
                "system_name": "my system",
                "tags": "my tags",
            }
        ]
        data_dst = Path(tempfile.gettempdir()) / "test_ingest_data"
        if data_dst.exists():
            shutil.rmtree(data_dst)
        data_dst.mkdir()
        try:
            ingest_data(results, db_file, data_dst, copy_files=False, max_workers=None)
            job_dirs = list(data_dst.iterdir())
            assert len(job_dirs) == 1
            metadata_file = data_dst / job_dirs[0] / "metadata.json"
            assert metadata_file.exists()
            metadata = json.loads(metadata_file.read_text())
            assert metadata["name"] == "job1"
        finally:
            shutil.rmtree(data_dst)

        con = sqlite3.connect(db_file)
        cur = con.cursor()
        res = cur.execute(f"SELECT * FROM {TABLE_NAME}")
        data = res.fetchall()
        assert len(data) == 1
        assert data[0][2] == "job1"

    print("tests passed")


if __name__ == "__main__":
    _run_tests()
