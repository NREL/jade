# Objective
Run a batch of jobs and ingest the results into a SQLite database.

## Jade configuration parameters
- Optimize compute node utilization. Based on per-job CPU and memory requirements, four jobs can be
run simulateously on a compute node. Node allocation times are long; run multiple rounds of jobs
once a node is allocated.
- Store global metadata about the jobs in the jade configuration.
- Store per-job metadata in each job's `ext` field.
- Store job output in the jade runtime output directory. Refer to `sim.jl` in this directory.
- Configure the jade `teardown_command` to run a script that ingests all successful results into a
SQLite database. Refer to `postprocess.py` and `ingest_data.py` in this directory. Note that
failures are detected and excluded.

## Steps
1. Create the jade configuration programmatically.
```
$ python create_config.py
```

2. Configure HPC parameters and Jade submission parameters. There are 8 jobs and each job takes
less than 30 minutes. The `short` queue has a walltime limit of 4 hours, and so run all jobs on one
node.
```
$ jade config hpc -t slurm --account=my_account --walltime="04:00:00"
Created HPC config file hpc_config.toml
$ jade config submitter-params --per-node-batch-size=8 --num-parallel-processes-per-node=4
Created submitter parameter file submitter_params.json
```

3. Submit the jobs.
```
jade submit-jobs -s params.json config.json
```

4. View the results files.
```
$ tree my_test_results
my_test_results
├── 1d432f9b-f4c3-483b-a606-5fe07dc21559
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 25c746f8-3c74-41c8-aeda-42e452a54c7d
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 26bae973-e793-4555-9ad7-33fe4fffddd9
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 616293d6-f323-4926-901d-12772d780c65
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 68f8cfee-315f-4e66-b333-9c05a6d90b0b
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 95b1c7d1-7731-4452-b768-a3be47b65353
│   ├── data
│   │   └── file.txt
│   └── metadata.json
├── 9662c8e6-43aa-4e66-acd9-e2368185c30a
│   ├── data
│   │   └── file.txt
│   └── metadata.json
└── my_results.db
```

```
jq . my_test_results/1d432f9b-f4c3-483b-a606-5fe07dc21559/metadata.json
{
  "name": "job_2",
  "path": "my_test_results/1d432f9b-f4c3-483b-a606-5fe07dc21559",
  "timestamp": "2022-11-17 08:58:57.305306",
  "exec_time_s": 10.038394927978516,
  "scenario": "my scenario",
  "system_name": "my system name",
  "tags": "my tags",
  "id": "1d432f9b-f4c3-483b-a606-5fe07dc21559",
  "user": "dthom"
}
```

5. Query the database with sqlite3.
```
$ sqlite3 -table my_test_results/my_results.db "select user, name, timestamp, path, scenario, tags from jobs"
+-------+-------+----------------------------+------------------------------------------------------+-------------+---------+
| user  | name  |         timestamp          |                         path                         |  scenario   |  tags   |
+-------+-------+----------------------------+------------------------------------------------------+-------------+---------+
| dthom | job_1 | 2022-11-17 08:58:57.304119 | my_test_results/616293d6-f323-4926-901d-12772d780c65 | my scenario | my tags |
| dthom | job_2 | 2022-11-17 08:58:57.305306 | my_test_results/1d432f9b-f4c3-483b-a606-5fe07dc21559 | my scenario | my tags |
| dthom | job_4 | 2022-11-17 08:58:57.307437 | my_test_results/25c746f8-3c74-41c8-aeda-42e452a54c7d | my scenario | my tags |
| dthom | job_5 | 2022-11-17 08:59:07.341774 | my_test_results/95b1c7d1-7731-4452-b768-a3be47b65353 | my scenario | my tags |
| dthom | job_6 | 2022-11-17 08:59:07.343259 | my_test_results/68f8cfee-315f-4e66-b333-9c05a6d90b0b | my scenario | my tags |
| dthom | job_7 | 2022-11-17 08:59:07.344358 | my_test_results/26bae973-e793-4555-9ad7-33fe4fffddd9 | my scenario | my tags |
| dthom | job_8 | 2022-11-17 08:59:07.345249 | my_test_results/9662c8e6-43aa-4e66-acd9-e2368185c30a | my scenario | my tags |
+-------+-------+----------------------------+------------------------------------------------------+-------------+---------+
```
