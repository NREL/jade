Tutorial
########
This page describes how to use the JADE package to create, modify, and run
jobs locally or on an HPC.

Installation
************
JADE can be installed on computer or HPC. If trying to install it on your machine,
you can choose to install with/without docker.

Computer with docker
=====================
Docker can run on different OS platforms - Linux, Mac, Windows, etc.
Please follow the document https://docs.docker.com/ to install Docker CE
on your machine first. Then you can continue JADE installation with docker.

1. Clone JADE source code to your machine.

::

    git clone git@github.nrel.gov:Hosting-Capacity-Analysis/jade.git

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

      Available commands for Job Automation and Deployment Engine (JADE)

    Options:
      --help  Show this message and exit.

    Commands:
      auto-config   Automatically create a configuration.
      extensions    Manage JADE extensions.
      show-events   Shows the events after jobs run.
      show-results  Shows the results of a batch of jobs.
      submit-jobs   Submits jobs for execution, locally or on HPC.

This base image is https://hub.docker.com/r/continuumio/miniconda3, which is built
on top of ``debian``, so you can use Linux commands for operation.

4. To exit docker environment, just type

::

    exit

For more about docker commands, please refer https://docs.docker.com/engine/reference/commandline/docker/.

Computer or HPC without docker
==============================

1. Install JADE. Choose the default environment for running jobs or the
   development environment if you will be developing new code or documentation.

.. note:: The dev environment requires that pandoc and plantuml be installed.

   - Refer to `pandoc <https://pandoc.org/installing.html>`_.
   - plantuml on Mac: ``brew install plantuml``
   - plantuml on Linux: ``sudo apt-get install plantuml``
   - plantuml on Windows: `plantuml <http://plantuml.com/starting>`_.

::

    git clone git@github.nrel.gov:Hosting-Capacity-Analysis/jade.git
    cd jade
    # If conda is not already in your environment (such as on HPC):
    module load conda

    conda env create -f environment.yml
    # or
    conda env create -f dev-environment.yml

    conda activate jade
    pip install -e .


Register extensions
*******************
An extension is a type of job that can be executed by JADE. Refer to
:ref:`extensions_label` for more information.

Register your extensions with your local JADE installation by entering the
command below.

::

   jade extensions register <EXTENSION_FILENAME>
   jade extensions show

JADE extensions are stored locally in ~/.jade-registry.json.

If all you want to do is batch a bunch of CLI commands then refer to
:ref:`generic_command_extension_label`.


CLI Execution
*************
Jade provides a CLI utility to start jobs.

submit-jobs
===========
Start execution of jobs defined in a configuration file.  If executed on an HPC
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
- **Per-node_batch size**: Number of jobs to run on one node in one batch.
- **Allocation time**: How long it takes to acquire a node. Dependent on the
  HPC queue chosen and the priority given.
- **Average job runtime**: How long it takes a job to complete.

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
        --output=output
        --max-nodes=20
        --per-node-batch-size=500
        --hpc-config=hpc_config_test.toml
        --verbose

.. note::

   By default HPC nodes are requested at normal priority. Set qos=high in
   hpc_config.json to get faster allocations at twice the cost.


Results
*******
View the results of the jobs.

::

    jade show-results --output=output

Or only the ones that failed::

    jade show-results --failed

Debugging
*********
JADE generates multiple log files that can help debug failures.

- ``submit_jobs.log``: HPC-related information, such as the job ID and status
- ``run_jobs.log``: information about JADE and Dask starting and stopping
  jobs
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
If your extensions implement structured JADE events then you may want to view
what events were logged.

::

    jade show-events
