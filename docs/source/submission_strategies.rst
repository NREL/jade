.. _submission_strategies:

*************************
Job Submission Strategies
*************************
This page provides examples on how to optimize JADE for different types of use
cases.

Independent, short, multi-core jobs
===================================

**Constraints**:

- Each job process consumes all cores of the compute node.
- Each job takes no more than 30 minutes.
- There are 1000 jobs.
- There are no job dependencies.
- Nodes on the short queue (4-hour max walltime) have a short acquisition time.
- Nodes on the standard queue have a long acquisition time.

**Strategy**:

- Define the walltime as 4 hours (for NREL/Eagle HPC) so that it will pick the
  ``short`` partition.
- Limit the number of parallel processes on each node (default is num CPUs).
- Set the per-node batch size to the number of jobs that can complete within
  the max walltime.
- Try to use as many nodes as possible in parallel.

**Command**:

.. code-block:: bash

    $ jade submit-jobs --num-processes=1 --per-node-batch-size=8 config.json

JADE will submit 125 single-node jobs to the HPC.

Independent, short, single-core jobs
====================================
Same as above except that each job consumes only one core.

Case 1
------

**Constraints**:

- A compute node has 36 cores.

**Strategy**:

- One compute node can complete 36 jobs every 30 minutes or 288 jobs in 4
  hours.
- 4 compute nodes are needed.

**Command**:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=250 config.json

JADE will submit 4 single-node jobs to the HPC.

.. note:: If you will have hundreds of thousands of jobs and hundreds of nodes,
   you may experience lock contention issues. If you aren't concerned with
   job-ordering then consider setting ``--no-distributed-submitter``.

Case 2
------
Same as case 1 but acquisition time is long on all queues.

**Strategy**:

- Acquire one node on the standard queue and then run all jobs on it.

**Command**:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=1000 config.json

Independent, single-core jobs with variable runtimes
====================================================
Some jobs complete in 10 minutes, some take 2 hours.

Case 1
------

**Constraints**:

- A compute node has 36 cores.

**Strategy**:

- One compute node can complete 36 jobs every 30 minutes or 288 jobs in 4
  hours.
- 4 compute nodes are needed.

**Command**:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=250 config.json

JADE will submit 4 single-node jobs to the HPC.

Case 2
------

**Constraints**:

- Each job process consumes one core of the compute node.
- Some jobs take 10 minutes, some take 2 hours.
- There are no job dependencies.
- Nodes on the short queue (4-hour max walltime) have a short acquisition time.
- Nodes on the standard queue have a long acquisition time.

**Strategy**:

- Define ``estimated_run_minutes`` for each job.
- Run ``jade submit-jobs`` with ``--time-based-batching`` and
  ``--num-processes=36``.
- Set the walltime value to 4 hours.
- JADE will build variable-sized batches based how many jobs can complete in 4
  hours on each node.

**Command**:

.. code-block:: bash

    $ jade submit-jobs --num-processes=36 --time-based-batching config.json

.. _submission_group_strategy:

Jobs that require different submission parameters
=================================================
Some jobs will take less than 4 hours, and so can run on the short queue. Other
jobs take longer and so need to run on the standard queue.

**Strategy**:

- Define two instances of a :ref:`model_submission_group`.
- Set the submission group for each job appropriately.

A submission group allows you to define batch parameters like
``per-node-batch-size`` as well as HPC parameters. You can customize most of
these parameters for each submission group.

Here's how to modify the existing ``config.json`` file.

1. Create default submission parameters with ``jade config submitter-params -c
   short-jobs.json``.
2. Customize the file as necessary.
3. Add those parameters as a submission group with
   ``jade config add-submission-group short-jobs.json short_jobs config.json``
4. Repeat steps 1-3 to create a group called ``long_jobs``.
5. Edit the ``submission_group`` field for each job in ``config.json`` to be
   one of the group names defined above.

Here is an example of part of a ``config.json`` file:

.. code-block:: json

    {
      "jobs": [
        {
          "command": "bash my_script.sh 1",
          "job_id": 1,
          "blocked_by": [],
          "extension": "generic_command",
          "append_output_dir": false,
          "cancel_on_blocking_job_failure": false,
          "estimated_run_minutes": null,
          "ext": {},
          "submission_group": "short_jobs"
        },
        {
          "command": "bash my_script.sh 2",
          "job_id": 2,
          "blocked_by": [],
          "extension": "generic_command",
          "append_output_dir": false,
          "cancel_on_blocking_job_failure": false,
          "estimated_run_minutes": null,
          "ext": {},
          "submission_group": "long_jobs"
        }
      ],
      "submission_groups": [
        {
          "name": "short_jobs",
          "submitter_params": {
            "hpc_config": {
              "hpc_type": "slurm",
              "job_prefix": "job",
              "hpc": {
                "account": "my_account",
                "walltime": "4:00:00"
              }
            },
            "per_node_batch_size": 500,
            "try_add_blocked_jobs": true,
            "time_based_batching": false
          }
        },
        {
          "name": "long_jobs",
          "submitter_params": {
            "hpc_config": {
              "hpc_type": "slurm",
              "job_prefix": "job",
              "hpc": {
                "account": "my_account",
                "walltime": "24:00:00"
              }
            },
            "per_node_batch_size": 500,
            "try_add_blocked_jobs": true,
            "time_based_batching": false
          }
        }
      ]
    }

Refer to :ref:`submission_group_behaviors` for additional information.

.. _multi_node_job_strategy:

Jobs that require multiple nodes
================================

.. note:: This is an experimental feature. Please let us know your feedback.

**Constraints**:

- A job needs 5 nodes.
- One node should become a manager that starts worker processes on all nodes.
- You have a script/program that can use all nodes.

**Strategy**:

Use JADE's multi-node manager to run your script.

- Set ``nodes = 5`` in the ``hpc_config.toml`` file.
- Set ``use_multi_node_manager = true`` for the job in the ``config.json``.
- The HPC will start JADE's manager script. JADE will assign the ``manager``
  role to the first node in the HPC node list. It will invoke your script,
  passing the runtime output directory and all node hostnames through
  environment variables.
- Your script uses all nodes to complete your work.

.. warning:: Be careful if you add more jobs to the config, such as for
   post-processing. Put them in a different submission group if they are
   single-node jobs.

Here is an example using a ``Julia`` script that uses the ``Distributed``
module to perform work on multiple nodes.

Contents of a script called ``run_jobs.jl``:

.. code-block:: julia

    using Distributed
    using Random

    function run_jobs(output_dir, hostnames)
        machines = [(x, i) for (i, x) in enumerate(hostnames)]
        addprocs(machines)
        @everywhere println("hello from $(gethostname())")

        results = [@spawnat i rand(10) for i in 1:length(hostnames)]
        for (i, result) in enumerate(results)
            res = maximum(fetch(result))
            println("Largest value from $(hostnames[i]) = $res")
        end
    end

    output = ENV["JADE_OUTPUT_DIR"]
    workers = split(ENV["JADE_COMPUTE_NODE_NAMES"], " ")
    isempty(workers) && error("no compute node names were set in JADE_COMPUTE_NODE_NAMES")

    run_jobs(output, workers)


**JADE job definition**:

.. code-block:: json

    {
      "command": "julia run_jobs.jl arg1 arg2",
      "job_id": 1,
      "blocked_by": [],
      "extension": "generic_command",
      "append_output_dir": true,
      "cancel_on_blocking_job_failure": false,
      "estimated_run_minutes": null,
      "use_multi_node_manager": true
    }

**HPC parameters**::

    hpc_type = "slurm"
    job_prefix = "job"

    [hpc]
    account = "my_account"
    walltime = "4:00:00"
    nodes = 5

JADE will set these environment variables:

- ``JADE_OUTPUT_DIR``: output directory passed to ``jade submit-jobs``
- ``JADE_COMPUTE_NODE_NAMES``: all compute node names allocated by the HPC

JADE will run the user command on the manager node when the HPC allocates the
nodes.

.. code-block:: bash

    $ julia run_jobs.jl arg1 arg2
