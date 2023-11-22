**************************
Frequently Asked Questions
**************************

- *How can I control the number of jobs that run simultaneously on each node?*

There are two options in ``jade submit-jobs`` that control parallelism:

1. ``--num-parallel-processes-per-node`` is the number of jobs that each node will run in parallel.
2. ``--per-node-batch-size`` is the total number of jobs that will be distributed to each node.

Ensure that the walltime value is large enough for each node to finish all jobs in its batch.


- *Can I save my options for jade submit-jobs to a file?*

Yes. Create your options similar to this example:

.. code-block:: bash

    $ jade config submitter-params --per-node-batch-size=8 --num-parallel-processes-per-node=2 -c params.json

- And then submit the jobs with

.. code-block:: bash

    $ jade submit-jobs -s params.json config.json


- *My jobs finished but I don't see the summary reports and output files in the output directory. What happened?*

The last compute node may have timed out. The Jade job runner process on the last node to complete its jobs
will complete the submission and generate final report files, including resource statistics (CPU/mem). If that
node experienced a walltime timeout, the final process won't run.

Run the command below to check. If this is what happened, it will run the final tasks. If you collected a
large amount of resource statistics, don't run this command on a login node as it may consume lots of memory
and CPU.

.. code-block:: bash

    $ jade show-status -o <output-dir>


- *How can I tell Jade to run one final post-processing job once all other jobs have finished?*

Define the ``teardown_command`` parameter in ``config.json``. Don't bother with defining job dependencies.


- *I submitted 10 sets of jobs at the same time. How can I distinguish my jobs in the output of squeue?*

Customize the ``job_prefix`` field in ``hpc_config.toml``. By default it is ``job`` and so you will see
``job_batch_1``, ``job_batch_2``, etc, in ``squeue``.


- *Can I check what will be submitted, and in which batch, without actually starting the jobs?*

Yes. There is a ``dry run`` feature.

.. code-block:: bash

    $ jade submit-jobs --dry-run config.json


- *Is there documentation for all of the jade commands?*

Not in the documentation. However, you can get help in the terminal.

.. code-block:: bash

    $ jade --help
    $ jade config --help
    $ jade config hpc --help
    $ jade submit-jobs --help
