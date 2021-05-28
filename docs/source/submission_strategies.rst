.. _submission_strategies:

*************************
Job Submission Strategies
*************************
This page provides examples on how to optimize JADE for different types of use
cases.

Independent, short, multi-core jobs
===================================

Constraints:

- Each job process consumes all cores of the compute node.
- Each job takes no more than 30 minutes.
- There are 1000 jobs.
- There are no job dependencies.
- Nodes on the short queue (4-hour max walltime) have a short acquisition time.
- Nodes on the standard queue have a long acquistion time.

Strategy:

- Define the walltime as 4 hours (for NREL/Eagle HPC) so that it will pick the
  ``short`` partition.
- Limit the number of parallel processes on each node (default is num CPUs).
- Set the per-node batch size to the number of jobs that can complete within
  the max walltime.
- Try to use as many nodes as possible in parallel.

Command:

.. code-block:: bash

    $ jade submit-jobs --num-processes=1 --per-node-batch-size=8 config.json

JADE will submit 125 single-node jobs to the HPC.

Independent, short, single-core jobs
====================================
Same as above except that each job consumes only one core.

Case 1
------

Constraints:

- A compute node has 36 cores.

Strategy:

- One compute node can complete 36 jobs every 30 minutes or 288 jobs in 4
  hours.
- 4 compute nodes are needed.

Command:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=250 config.json

JADE will submit 4 single-node jobs to the HPC.

Case 2
------
Same as case 1 but acquistion time is long on all queues.

Strategy:

- Acquire one node on the standard queue and then run all jobs on it.

Command:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=1000 config.json

Independent, single-core jobs with variable runtimes
====================================================
Some jobs complete in 10 minutes, some take 2 hours.

Case 1
------

Constraints:

- A compute node has 36 cores.

Strategy:

- One compute node can complete 36 jobs every 30 minutes or 288 jobs in 4
  hours.
- 4 compute nodes are needed.

Command:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=250 config.json

JADE will submit 4 single-node jobs to the HPC.

Case 2
------

Constraints:

- Each job process consumes one core of the compute node.
- Some jobs take 10 minutes, some take 2 hours.
- There are no job dependencies.
- Nodes on the short queue (4-hour max walltime) have a short acquisition time.
- Nodes on the standard queue have a long acquistion time.

Strategy:

- Define ``estimated_run_minutes`` for each job.
- Run ``jade submit-jobs`` with ``--time-based-batching`` and
  ``--num-processes=36``.
- Set the walltime value to 4 hours.
- JADE will build variable-sized batches based how many jobs can complete in 4
  hours on each node.

Command:

.. code-block:: bash

    $ jade submit-jobs --num-processes=36 --time-based-batching config.json

.. _submission_group_strategy:

Jobs that require different submission parameters
=================================================
Some jobs will take less than 4 hours, and so can run on the short queue. Other
jobs take longer and so need to run on the standard queue.

Strategy:

- Define two submission groups.
- Set the submission group for each job appropriately.

The ``SubmissionGroup`` object contains a name as well as a
``SubmitterParameters`` object. The latter contains batch parameters like
``per-node-batch-size`` as well as HPC parameters. You can customize most of
these parameters for each submission group.

Here's how to create the JSON object that you will need to add to the
``config.json`` file.

1. Create default submission parameters with ``jade config submitter-params -c submitter_params.json``.
2. Edit that file and make the single object an array of objects (enclose it with `[]`).
3. Make a copy of the object for each group.
4. Customize each group.
5. Add the group to the ``config.json`` file at the root level.
6. Add a ``submission_group`` entry to each job where the value is the group name.

Here is an example of part of a ``config.json`` file:

.. code-block:: json

    {
      "jobs": [
        {
          "command": "bash myscript.sh 1",
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
          "command": "bash myscript.sh 2",
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

Restrictions
------------
The following parameters must be the same across all submission groups:

- ``max_nodes``
- ``poll_interal``

The following parameters in the submission groups will be overridden by the
parameters specified on the command line for ``jade submit-jobs``:

- ``dry_run``
- ``generate_reports``
- ``resource_monitor_interval``
- ``verbose``

The following parameters in the submission groups will be overridden by the
parameters specified on the command line for ``jade submit-jobs`` **only**
if they are not set for the group:

- ``node_setup_script``
- ``node_shutdown_script``

The following parameters are completely controlled by the group:

- ``hpc_config``
- ``num_processes``
- ``per_node_batch_size``
- ``time_based_batching``
- ``try_add_blocked_jobs``
