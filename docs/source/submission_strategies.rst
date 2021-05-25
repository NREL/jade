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
