
**********
Spark Jobs
**********

JADE has functionality to create an ephemeral Spark cluster on HPC compute nodes and then run one
or more jobs on that cluster.

**Prerequisite**: The Spark software must be installed inside a Singularity container.

Setup
=====
1. Create a Docker container with Spark installed. Here is an `example
   <https://github.com/dsgrid/dsgrid/blob/main/Dockerfile>`_.
2. Convert that container to Singularity and copy the image to the HPC's shared file system.
3. Follow the normal steps to create your Jade configuration, such as with ``jade config create
   commands.txt``. The commands should be the scripts that you want to run against the Spark
   cluster including any command-line arguments. **Note**: Jade will append two positional
   arguments to your command line arguments: the spark cluster (``spark://<node_name>:7077``) and the
   job output directory.
4. Create your HPC config file with ``jade config hpc -c hpc_config.toml``. Set ``nodes`` in
   ``hpc_config.toml`` to be the number of compute nodes you want to participate in the Spark
   cluster. Spark will perform poorly if its scratch file space is on slow storage. You should
   specify requirements here that give you nodes with fast internal storage. On NREL's Eagle
   HPC cluster that is only the big-memory and CPU nodes. Alternatively, you can use nodes' tmpfs
   for scratch space as described below.
5. Run the command below to update the configuration with Spark parameters. Refer to ``--help`` for
   additional options. This will produce spark configuration files in ``./spark/conf`` that you
   can customize. One possible customization is ``spark.sql.shuffle.partititons`` and 
   ``spark.default.parallelism`` in ``spark/conf/spark-defaults.conf``. Jade sets them to the total
   number of cores in the cluster by default. Refer to Spark documentation for help with the
   parameters.

::

    $ jade config spark -c <path-to-container> -h hpc_config.toml --update-config-file=config.json

6. If you set a custom memory requirement in your ``hpc_config.toml`` then Jade will increase the
   ``spark.executor.memory`` value in ``spark/conf/spark-defaults.conf``. The default value is
   intended to maximize memory for 7 executors on each compute node. Customize as needed.
7. View the changes to your ``config.json`` if desired.
8. If you are using compute nodes with slow internal storage, consider setting ``use_tmpfs_for_scratch``
   to true. Note that this reduces availabe worker memory by half and you'll need to adjust
   ``spark.executor.memory`` accordingly. You can set the option in ``config.json`` or by passing
   ``-U`` to the ``jade config spark`` command.
9. Consider whether you want your jobs to be run inside or outside the container (default is inside).
   Jade will run each job outside the container if the ``run_user_script_outside_container`` option is
   set to true. You can set the option for each job in ``config.json`` or by passing ``-r`` to
   the ``jade config spark`` command. If you do run your script outside of the container, Jade will
   still set Spark environment variables to point to your local, customizable Spark config
   directory.
10. Set ``collect_worker_logs`` to true if your jobs are getting logs of errors. These can grow large.
11. Submit the jobs with ``jade submit-jobs config.json``. Jade will create a new cluster for each
    job, running them sequentially.

Run scripts manually
====================
In some cases you may prefer that JADE setup the cluster and then go to sleep while you ssh to a compute
node and run scripts manually.

1. Make a script called ``sleep.sh`` with the content below. This time of 59 minutes will allow JADE to
   cleanly shutdown the cluster if there is a 1-hour wall-time timeout.

.. code:: bash

   #!/bin/bash
   sleep 59m

2. Set the JADE command in ``commands.txt``/``config.json`` to be ``bash sleep.sh``.

3. Setup the environment. Before running your scripts it is important that you set the
``SPARK_CONF_DIR`` environment variable so that your SparkSession gets initialized with
the correct parameters.

This example assumes that your JADE output directory is ``./output`` and there is one job named ``1``.

.. code-block:: bash

   $ ssh <first-compute-node-name>
   $ cd <wherever-you-started-the-jade-jobs>
   $ export SPARK_CONF_DIR=./output/job-outputs/1/spark/conf

4. Run your scripts. Here is one way in Python to create a SparkSession.

.. code-block:: bash

   from pyspark.sql import SparkSession
   from pyspark import SparkConf, SparkContext
   cluster = "<first-compute-node-name>:7077"
   conf = SparkConf().setAppName("my_session").setMaster(cluster)
   spark = SparkSession.builder.config(conf=conf).getOrCreate()


Run a Jupyter server
====================
This example shows how to make JADE start a Jupyter server with the environment ready to use the Spark
cluster.

1. Create a bash script with the content below. Save the script as ``start_notebook.sh``.

.. code-block:: bash

   #!/bin/bash
   unset XDG_RUNTIME_DIR
   export SPARK_CLUSTER=$1
   echo "Spark cluster is running at ${SPARK_CLUSTER}" >&2
   echo "JADE output directory is ${2}" >&2
   jupyter notebook --no-browser --ip=0.0.0.0 --port 8889 &
   sleep 10
   echo "Create an ssh tunnel with this command: ssh -L 8889:${HOSTNAME}:8889 -L 8080:${HOSTNAME}:8080 -L 4040:${HOSTNAME}:4040 ${USER}@el1.hpc.nrel.gov" >&2
   wait

2. Set the JADE command in ``commands.txt``/``config.json`` to be ``bash start_notebook.sh``.

3. Submit the jobs with ``jade submit-jobs config.json -o output``

4. Once the job is allocated run ``tail -f output/*.e``. After 15-20 seconds you will see console
   output from the script above telling you how to create the ssh tunnel required to connect to the
   Jupyter server. You will also see console output from Jupyter that contains a URL.

5. Open the ssh tunnel.

6. Connect to the Jupyter server from your browser.

7. Create a SparkSession and start running your code. An example is below. You probably will want
   to split these into two cells. **Note** that this reads the Spark cluster name from the
   environment.

.. code-block:: python

   import os
   from IPython.core.display import display, HTML
   from pyspark.sql import SparkSession
   from pyspark import SparkConf, SparkContext
   display(HTML("<style>.container { width:100% !important; }</style>"))

   cluster = os.environ["SPARK_CLUSTER"]
   conf = SparkConf().setAppName("my_session").setMaster(cluster)
   spark = SparkSession.builder.config(conf=conf).getOrCreate()

8. Connect to the Spark UI from your browser, if desired, to monitor your jobs.

http://localhost:4040 and/or http://localhost:8080

9. If you want to ensure that JADE shuts down the Spark cluster cleanly (preserving history)
   then you should shutdown the notebook. ssh to the first compute-node and run
   ``jupyter notebook stop 8889``.


Debugging Problems
==================
Jade stores Spark logs, events, and metrics in ``<output-dir>/job-outputs/<job-id>/spark``.

You can browse the job details in the Spark UI by starting a Spark history server pointed to one
of the job output directories. You can do this on your local computer or on the HPC. If you do it
on the HPC then you'll need to create an ssh tunnel to the compute node and forward the port 18080.

Here is an example where the files are on your local system::

    $ SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=output/job-outputs/1/spark/events" $SPARK_HOME/sbin/start-history-server.sh

Load the Spark UI by opening your browser to http://localhost:18080

Compute Node Resource Monitoring
================================
It can be very helpful to collect CPU, memory, disk, and network resource utilization statistics
for all compute nodes. Refer to :ref:`resource_monitoring` for how to configure Jade to do this for
you.
