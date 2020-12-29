********
Tutorial
********

This page describes how to use the JADE package to create, modify, and run
jobs locally or on HPC.

HPC Configuration
=================
This section only applies if you run your jobs on HPC.

HPC Parameters
--------------
JADE will submit jobs to the HPC with parameters defined in
``hpc_config.toml``.  Create a copy and customize according to your needs.

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

Prerequistes
------------
If you are not using the JADE conda environment then you should take note of
the packages it installs (environment.yml). One common pitfall is that JADE
requires a newer version of git than some users have.


Configuring Jobs
================
A JADE configuration contains a list of jobs to run. Configurations can also be 
created manually or programmatically. JADE implements a CLI command to simplify 
the interface for the commonly-executed  ``generic_command`` extension behind.

Job Commands
------------

.. code-block:: bash

    $ jade config create <commands-file> -c config.json

Where ``commands-file`` is a text file with a list of commands to execute and 
JADE will run them in parallel. ``config.json`` contains each job definition.

Job Ordering
------------
Each job defines a ``blocked_by`` field. If you want to guarantee that job ID
2 doesn't run until job ID 1 completes then add that ID to the field.

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
    },
    {
      "command": "<job_cli_command4>",
      "job_id": 4,
      "blocked_by": [2, 3]
    }

Custom Extension (Optional)
---------------------------

If you are creating a customized JADE extension, it is recommended to provide
an ``auto-confg`` method that will automatically create a configuration with
all possible jobs.  If that is in place then this command will create the
configuration.

.. code-block:: bash

    $ jade auto-config <extension-name> <input_path> -c config.json

For more details about how to create a custom extension, please refer to 
:ref:`advanced_guide_label`.


CLI Execution
=============
Jade provides a CLI utility to start jobs.

submit-jobs
-----------
Start execution of jobs defined in a configuration file.  If executed on HPC
this will submit the jobs to the HPC queue. Otherwise, it will run the jobs
locally.

.. note::

   If running on the HPC then you should start jobs from a `tmux
   <https://github.com/tmux/tmux/wiki>`_ or `screen
   <https://www.gnu.org/software/screen>`_ session so that the job manager
   stays alive if you disconnect from the network.

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

If the jobs are very quick to execute and it takes a long time to acquire a
node then you may be better off making per_node_batch_size higher and max_nodes
lower.

Conversely, if the jobs take a long time then you may want to do the opposite.

Run ``jade submit-jobs --help`` to see defaults.

Examples::

    # Use defaults.
    $ jade submit-jobs config.json

    # Specify options.
    $ jade submit-jobs config.json \
        --output=output \
        --max-nodes=20 \
        --per-node-batch-size=500 \
        --hpc-config=hpc_config.toml

.. note::

   By default HPC nodes are requested at normal priority. Set qos=high in
   hpc_config.toml to get faster allocations at twice the cost.


Job Results
===========
View the results of the jobs.

.. code-block:: bash

    $ jade show-results --output=output

Or only the ones that failed

.. code-block:: bash

    $ jade show-results --failed

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
JADE automatically monitors CPU, disk, memory, and network utilization
statistics in structured log events.  Use this CLI command to view them,

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
   viewer = CpuStatsViewer(summary.events)
   cpu_df =  viewer.get_dataframe("resource_monitor_batch_0")
   cpu_df.head()
