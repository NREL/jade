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

- Use the short queue (``partition = "short"`` in ``hpc_config.toml``).
- Limit the number of parallel processes on each node (default is num CPUs).
- Set the per-node batch size to the number of jobs that can complete within
  the max walltime.
- Try to use as many nodes as possible in parallel.

Command:

.. code-block:: bash

    $ jade submit-jobs --num-processes=1 --per-node-batch-size=8 --max-nodes=125 config.json


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

    $ jade submit-jobs --per-node-batch-size=250 --max-nodes=4 config.json


Case 2
------
Same as case 1 but acquistion time is long on all queues.

Strategy:

- Acquire one node on the standard queue and then run all jobs on it.

Command:

.. code-block:: bash

    $ jade submit-jobs --per-node-batch-size=1000 --max-nodes=1 config.json
