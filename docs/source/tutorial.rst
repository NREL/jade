Tutorial
########
This page describes how to use the JADE package to create, modify, and run
jobs locally or on an HPC.

Installation
************
JADE can be installed on your computer or HPC. If trying to install it on your
computer, you can choose to install it in a conda environment or Docker
container.

Computer or HPC in conda environment
====================================
1. Clone JADE. ::

    git clone git@github.com:NREL/jade.git
    cd jade

2. Choose a virtual environment in which to install JADE.  This can be an
   existing `conda <https://docs.conda.io/en/latest/miniconda.html>`_
   environment or an environment from something like `pyenv
   <https://github.com/pyenv/pyenv>`_.  A validated conda environment is
   provided in the JADE repository. ::

    conda env create -f environment.yml -n jade
    conda activate jade

3. Install JADE. ::

    pip install -e .

    # If you will also be developing JADE code then include dev packages.
    pip install -e . -r dev-requirements.txt

.. note:: The dev packages require that pandoc and plantuml be installed.

   - Refer to `pandoc <https://pandoc.org/installing.html>`_.
   - plantuml on Mac: ``brew install plantuml``
   - plantuml on Linux: ``sudo apt-get install plantuml``
   - plantuml on Windows: `plantuml <http://plantuml.com/starting>`_.


Computer with docker
=====================
Docker can run on different OS platforms - Linux, Mac, Windows, etc.
Please follow the document https://docs.docker.com/ to install Docker CE
on your machine first. Then you can continue JADE installation with docker.

1. Clone JADE source code to your machine.

::

    git clone git@github.com:NREL/jade.git

2. Build ``jade`` docker image

::

    docker build -t jade .

3. Run ``jade`` docker container

::

    docker run --rm -it -v absolute-input-data-path:/data jade

After the container starts, the terminal will show something like this

::

    (jade) root@d14851e20888:/data#

Then type ``jade`` to show JADE related commands

::

    (jade) root@d14851e20888:/data# jade

    Usage: jade [OPTIONS] COMMAND [ARGS]...

      JADE commands

    Options:
      --help  Show this message and exit.

    Commands:
      auto-config   Automatically create a configuration.
      config        Manage a JADE configuration.
      extensions    Manage JADE extensions.
      pipeline      Manage JADE execution pipeline.
      show-events   Shows the events after jobs run.
      show-results  Shows the results of a batch of jobs.
      stats         View stats from a run.
      submit-jobs   Submits jobs for execution, locally or on HPC.

This base image is https://hub.docker.com/r/continuumio/miniconda3, which is
built on top of ``debian``, so you can use Linux commands for operation.

4. To exit docker environment, just type

::

    exit

For more about docker commands, please refer https://docs.docker.com/engine/reference/commandline/docker/.

Register extensions
*******************
An extension is a type of job that can be executed by JADE. Refer to
:ref:`extensions_label` for more information.

Register your extensions with your local JADE installation by entering the
command below.

::

    jade extensions register <EXTENSION_FILENAME>
    jade extensions show

If you're using a Python package other than JADE then you will likely also want
to register it as a package that JADE logs.  Here's how to do that::

    jade extensions add-logger <package-name>

JADE extensions are stored locally in ~/.jade-registry.json.

If all you want to do is batch a list of CLI commands then refer to
:ref:`generic_command_extension_label`.


HPC Configuration
*****************
This section only applies if you run your jobs on an HPC.

HPC Parameters
==============
JADE will submit jobs to the HPC with parameters defined in
``hpc_config.toml``.  Create a copy and customize according to your needs.

Lustre Filesystem
=================
If you are running on a Lustre filesystem then you should consider whether to
configure the Lustre stripe count. This can be beneficial if the the files you
create will be large or if many clients will be accessing them concurrently.

References:

- http://wiki.lustre.org/Configuring_Lustre_File_Striping
- https://www.nics.tennessee.edu/computing-resources/file-systems/lustre-striping-guide

.. note::

   This example Lustre filesystem command will only work if the directory is
   empty.

::

    lfs setstripe -c 16 <run-directory>

Prerequistes
============
If you are not using the JADE conda environment then you should take note of
the packages it installs (environment.yml). One common pitfall is that JADE
requires a newer version of git than users have.

Configuring Jobs
****************
A JADE configuration contains a list of jobs to run. Each configuration is
specific to the extension you are using. Extensions are recommended to provide
an ``auto-confg`` method that will automatically create a configuration with
all possible jobs.  If that is in place then this command will create the
configuration::

    jade auto-config <extension-name> -c config.json

``config.json`` contains each job definition.

Configurations can also be created manually or programmatically. Extensions
may provide methods to create configurations with a subset of possible jobs.

JADE implements a CLI command to simplify the interface for the
commonly-executed generic_command extension.

::

    jade config create <commands-file> -c config.json

CLI Execution
*************
Jade provides a CLI utility to start jobs.

submit-jobs
===========
Start execution of jobs defined in a configuration file.  If executed on an HPC
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
    jade submit-jobs config.json

    # Specify options.
    jade submit-jobs config.json \
        --output=output \
        --max-nodes=20 \
        --per-node-batch-size=500 \
        --hpc-config=hpc_config_test.toml \
        --verbose

.. note::

   By default HPC nodes are requested at normal priority. Set qos=high in
   hpc_config.toml to get faster allocations at twice the cost.


Results
*******
View the results of the jobs.

::

    jade show-results --output=output

Or only the ones that failed::

    jade show-results --failed

Debugging
*********
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

::

    find output -name "*.log" -o -name "*.e"
    output/J1__3__1.15__1.0__deployment1.dss/logs/deployment1.dss_simulation.log
    output/J1__3__1.15__1.0__deployment1.dss/pydss-project/Logs/pydss-project_deployment1.dss.log
    output/submit_jobs.log
    output/job_output_1151157.e

Useful grep commands::

    grep "WARNING\|ERROR" output/*log
    grep -n "srun\|slurmstepd\|Traceback" output/*.e

Events
======
If your extension implements JADE structured log events then you may want to
view what events were logged.

JADE will also log any unhandled exceptions here.

::

    jade show-events
    jade show-events -c Error


Resource Monitoring
===================
JADE automatically monitors CPU, disk, memory, and network utilization
statistics in structured log events.  Use this CLI command to view them::

    jade stats show
    jade stats show cpu
    jade stats show disk
    jade stats show mem
    jade stats show net

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
