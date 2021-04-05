********
Tutorial
********

This page describes how to use the JADE package to create, modify, and run
jobs locally or on HPC.

HPC Configuration
=================
This section only applies if you run your jobs on HPC.

Change to a directory on the shared filesystem (such as /scratch on Eagle).
JADE uses the filesystem for internal synchronization.

HPC Parameters
--------------
You must define your HPC configuration in settings file. Run this command
customized to your parameters.

.. code-block:: bash

    $ jade config hpc -a my-project -p short -t slurm -w "4:00:00" -c hpc.toml
    Created HPC config file hpc_config.toml

All parameters have defaults, and so you can run ``jade config hpc`` and then
edit the file afterwards.

Lustre Filesystem
-----------------
If you are running on a Lustre filesystem then you should consider whether to
configure the Lustre stripe count. This can be beneficial if the the files you
create will be large or if many clients will be accessing them concurrently.

References:

- http://wiki.lustre.org/Configuring_Lustre_File_Striping
- https://www.nics.tennessee.edu/computing-resources/file-systems/lustre-striping-guide

.. note::

   This example Lustre filesystem command will only work if the directory is
   empty.

.. code-block:: bash

    $ lfs setstripe -c 16 <run-directory>

Configuring Jobs
================
Create a text file with a list of commands, one per line.

.. code-block:: bash

    $ cat commands.txt
    bash my_script.sh ARG1
    bash my_script.sh ARG2

Job Commands
------------

.. code-block:: bash

    $ jade config create commands.txt -c config.json
    Created configuration with 2 jobs.
    Dumped configuration to config.json.

JADE created ``config.json`` with one definition per job. You can edit this
file to customize execution behavior.

Job Ordering
------------
Each job defines a ``blocked_by`` field. If you want to guarantee that job ID
2 doesn't run until job ID 1 completes then add that ID to the field.

If a job that is blocking another job fails then JADE will still run the
blocked job by default. This may not be what you want. You can optionally set
``cancel_on_blocking_job_failure`` to cause those blocked jobs to be canceled.

.. code:: python

    {
      "command": "<job_cli_command1>",
      "job_id": 1,
      "blocked_by": []
    },
    {
      "command": "<job_cli_command2>",
      "job_id": 2,
      "blocked_by": [1]
    },
    {
      "command": "<job_cli_command3>",
      "job_id": 3,
      "blocked_by": [1]
      "cancel_on_blocking_job_failure": false
    },
    {
      "command": "<job_cli_command4>",
      "job_id": 4,
      "blocked_by": [2, 3],
      "cancel_on_blocking_job_failure": true
    }


Show Job Summary
----------------
In order to view a summary of your jobs in a table:

.. code:: bash

    $ jade config show config.json

    Num jobs: 4
    +-------+------+------------+
    | index | name | blocked_by |
    +-------+------+------------+
    |   0   |  1   |            |
    |   1   |  2   |     1      |
    |   2   |  3   |     1      |
    |   3   |  4   |    2, 3    |
    +-------+------+------------+

CLI Execution
=============
Jade provides a CLI utility to start jobs.

submit-jobs
-----------
Start execution of jobs defined in a configuration file.  If executed on HPC
this will submit the jobs to the HPC queue. Otherwise, it will run the jobs
locally.

It's important to understand how JADE submits HPC jobs in order to optimize
your performance.  JADE divides the jobs created by the user into batches.  It
makes one HPC node submission for each batch. Once running on a node it runs in
parallel a number of worker processes equal to the number of CPUs on that node
(36 on Eagle).

Parameters to keep in mind:

- **Number of jobs**: Number of jobs created by the user.
- **Max nodes**: Max number of job submissions (batches) to run in parallel.
- **Per-node batch size**: Number of jobs to run on one node in one batch.
- **Allocation time**: How long it takes to acquire a node. Dependent on the
  HPC queue chosen and the priority given.
- **Average job runtime**: How long it takes a job to complete.
- **HPC config file**: Customized HPC parameters like walltime and partition
- **Time-based batching**: If jobs have variable runtimes then it is better to
  define those runtimes in the config file and then use the
  ``--time-based-batching`` flag to let JADE create variable-sized batches.
  Mutually exclusive with --per-node-batch-size.

If the jobs are very quick to execute and it takes a long time to acquire a
node then you may be better off making per_node_batch_size higher and max_nodes
lower. Conversely, if the jobs take a long time then you may want to do the
opposite.

Refer to :ref:`submission_strategies` for a description of how to handle
specific use cases.

Examples::

    # Use defaults.
    $ jade submit-jobs config.json

    # Specify options.
    $ jade submit-jobs config.json \
        --output=output \
        --max-nodes=20 \
        --per-node-batch-size=500 \
        --hpc-config=hpc_config.toml

Run ``jade submit-jobs --help`` to see all command options and defaults.

To aid with repeated runs you can pass these parameters in a config file.
Generate the defaults with

.. code-block:: bash

    $ jade config submitter-params

    Created submitter parameter file submitter_params.toml

And then pass this file to ``submit-jobs``

.. code-block:: bash

    $ jade submit-jobs config.json -s submitter_params.toml

.. note::

   By default HPC nodes are requested at normal priority. Set qos=high in
   hpc_config.toml to get faster allocations at twice the cost.


Output Directory
----------------
JADE stores all of its configuration information and log files in the output
directory specified by the ``submit-jobs`` command. You can tell JADE to
forward this directory to the job CLI commands by setting the
``append_output_dir`` job parameter to true.

Suppose you submit jobs with

.. code-block:: bash

    jade submit-jobs config.json -o output

Where ``config.json`` contains a job definition like this:

.. code-block:: json

    {
      "command": "bash my_script.sh",
      "job_id": 1,
      "blocked_by": [],
      "append_output_dir": true
    }

JADE will actually invoke this:

.. code-block:: bash

    $ bash my_script.sh --jade-runtime-output=output

This can be useful to collect all job outputs in a common location. JADE
automatically creates ``<output-dir>/job-outputs`` for this purpose.

Node setup and shutdown scripts
-------------------------------
When running on an HPC you might want to copy input files to each compute node
before running jobs and then upload output data afterwards. JADE provides
options to automate this process.

.. code-block:: bash

    jade submit-jobs --node-setup-script="python setup_node.py" --node-shutdown-script="python shutdown_node.py" config.json

In this example JADE will invoke these commands on each compute node.

.. code-block:: bash

    python setup_node.py config_batch1.json output-dir
    python shutdown_node.py config_batch1.json output-dir

Note the arguments:

1. JADE config file for that node's batch. It contains only the jobs in the batch.
2. the output directory passed to ``jade submit-jobs``

You can use this information to decide what files to copy. Here is an example
of how to use it.

Pre-requisite: define required files for each in each job's ``ext`` field.

.. code-block:: json

    {
      "command": "bash my-script.sh",
      "job_id": 1,
      "blocked_by": [],
      "extension": "generic_command",
      "ext": {"required_files": ["/projects/X/input_data.json"]}
    }

Here is what you can do in the setup script.

.. code-block:: python

    import os
    import shutil
    import sys
    from pathlib import Path

    from jade.jobs.job_configuration_factory import create_config_from_file

    config_file = sys.argv[1]
    config = create_config_from_file(config_file)
    required_files = set()
    for job in config.iter_jobs():
        required_files.update(set(job.ext.get("required_files", [])))

    work_dir = os.environ["LOCAL_SCRATCH"]  # or whatever is appropriate for your environment
    for filename in required_files:
        shutil.copyfile(filename, Path(work_dir) / os.path.basename(filename))


Job Execution
=============

HPC
---
The job submitter runs in a distributed fashion across the login node and all
compute nodes that get allocated.

1. User initiates execution by running ``jade submit-jobs`` on the login node.
2. JADE submits as many batches as possible and then exits. Jobs can be blocked
   by ordering requirements or the user-specified max-node limit.
3. HPC queueing system allocates a compute node for a batch of jobs and starts
   the JADE job runner process.
4. Both before and after running a batch of jobs the job runner will run
   ``jade try-submit-jobs``. If it finds newly-unblocked jobs then it will
   submit them in a new batch. This will occur on every allocated compute node.
5. When a submitter detects that all jobs are complete it will summarize
   results and mark the configuration as complete.

The JADE processes synchronize their activity with status files and a file lock
in the output directory.

Local
-----
JADE runs all jobs at the specified queue depth until they all complete.

Job Status
===========
While jobs are running you can check status with this command:

.. code-block:: bash

    $ jade show-status -o output

The status is updated when each compute node starts or completes its execution
of a batch, so this status may not be current.

Every job runner will log completions to the same file, so you can see live job
completions with this command. Note that this file is cleared each time a
submitter processes it.

.. code-block:: bash

    $ tail -F output/results.csv

Check processed jobs in this file:

.. code-block:: bash

    # Find out how many have completed.
    $ wc -l output/processed_results.csv

    # Follow updates.
    $ tail -f output/processed_results.csv


Every submitter will log to the same file, so you can monitor submission status
with this command:

.. code-block:: bash

    $ tail -f output/submit-jobs.log

You can also trigger a full status update by manually trying to submit new
jobs.

.. code-block:: bash

    $ jade try-submit-jobs output
    $ jade show-status -o output


Job Results
===========
Once execution is complete you can view the results of the jobs.

.. code-block:: bash

    $ jade show-results --output=output

Or only the ones that failed

.. code-block:: bash

    $ jade show-results --failed

Failed or Missing Jobs
======================
If some jobs fail because of a walltime timeout or code/data error then you can
resubmit those specific jobs without re-running all the jobs that passed.

Jobs that timeout will be reported as missing.

.. code-block:: bash

    $ jade resubmit-jobs --missing --failed output

.. note:: This command is currently not supported in local mode.

Debugging
=========
By default JADE generates report files that summarize what happened. Refer to
``results.txt``, ``errors.txt``, and ``stats.txt``. The results file shows
whether each job passed or failed.  The errors file shows unhandled errors
that JADE detected as well as known errors that it parsed from log files.

Here are the log files that JADE generates. Open these to dig deeper.

- ``submit_jobs.log``: HPC-related information, such as the job ID and status
- ``run_jobs.log``: information about JADE starting and stopping jobs
- ``job_output_<HPC job ID>.e``: The HPC logs stdout and stderr from all
  processes to this file. Look here to debug unexpected crashes or hangs.

  - Python crashes will print ``Traceback`` to stderr, so that is a good string
    to search for.
  - Search for SLURM errors:  ``srun``, ``slurmstepd``, ``DUE TO TIME LIMIT``

.. code-block:: bash

    $ find output -name "*.log" -o -name "*.e"
    output/J1__3__1.15__1.0__deployment1.dss/logs/deployment1.dss_simulation.log
    output/J1__3__1.15__1.0__deployment1.dss/pydss-project/Logs/pydss-project_deployment1.dss.log
    output/submit_jobs.log
    output/job_output_1151157.e

Useful grep commands

.. code-block:: bash

    $ grep "WARNING\|ERROR" output/*log
    $ grep -n "srun\|slurmstepd\|Traceback" output/*.e

Events
------
If your extension implements JADE structured log events then you may want to
view what events were logged.

JADE will also log any unhandled exceptions here.

.. code-block:: bash

    $ jade show-events
    $ jade show-events -c Error


Resource Monitoring
-------------------
JADE optionally monitors CPU, disk, memory, and network utilization
statistics in structured log events. You can enable the feature by passing
``-rX`` or ``--resource-monitor-interval=X`` where ``X`` is an interval in
seconds.

If reports are enabled (in ``submit-jobs``) then JADE will save interactive
plots of the this data in ``<output-dir>/stats``.

.. code-block:: bash

    $ tree output/stats
    output/stats
    ├── CpuStatsViewer__resource_monitor_batch_0.html
    ├── DiskStatsViewer__resource_monitor_batch_0.html
    ├── MemoryStatsViewer__resource_monitor_batch_0.html
    └── NetworkStatsViewer__resource_monitor_batch_0.html

Use this CLI command to view textual tables after a run:

.. code-block:: bash

    $ jade stats show
    $ jade stats show cpu
    $ jade stats show disk
    $ jade stats show mem
    $ jade stats show net

.. note:: Reads and writes to the Lustre filesystem on the HPC are not tracked.

The stats can also be provided as pandas.DataFrame objects. For example, here
is how to view CPU stats for the node that ran the first batch:

.. code-block:: python

   from jade.events import EventsSummary, EVENT_NAME_CPU_STATS
   from jade.resource_monitor import CpuStatsViewer

   summary = EventsSummary("output")
   viewer = CpuStatsViewer(summary)
   for name in viewer.iter_batch_names():
       cpu_df =  viewer.get_dataframe(name)
       print(cpu_df.head())

Deadlocks
---------
While it should be very rare, it is possible that JADE gets deadlocked and
stops submitting jobs. When a compute node finishes a batch of jobs it acquires
a file lock in order to update status and attempt to submit new jobs. This
should usually take less than one second. If a walltime timeout occurs while
this lock is held and the JADE process is terminated then no other node will be
able to promote itself to submitter and jobs will be stuck.

We plan to add code to detect this condition and resolve it in the future. If
this occurs you can fix it manually by deleting the lock file and restarting
jobs.

.. code-block:: bash

    $ rm <output-dir>/cluster_config.json.lock
    $ jade try-submit-jobs <output-dir>
