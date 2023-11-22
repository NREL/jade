
**********
Spark Jobs
**********

JADE has functionality to create an ephemeral Spark cluster on HPC compute nodes and then run one
or more jobs on that cluster.

**Prerequisite**: The Spark software must be installed inside a Singularity container.

Setup
=====
1. Create a Docker container with Spark installed. Here is an `example
   <https://github.com/NREL/jade/blob/main/jade/spark/Dockerfile>`_. That image includes
   Nvidia GPU support. Note that this container uses Python 3.8 and Spark 3.2.1. You may hit
   incompatibilities if you run jobs against the cluster with different versions.
2. Convert that container to Singularity and copy the image to the HPC's shared file system.
3. Follow the normal steps to create your Jade configuration, such as with ``jade config create
   commands.txt``. The commands should be the scripts that you want to run against the Spark
   cluster including any command-line arguments. **Note**: Jade will append two positional
   arguments to your command line arguments: the spark cluster (``spark://<node_name>:7077``) and the
   job output directory. Here is an example script::

.. code-block:: bash

   !/bin/bash
   SPARK_CLUSTER=$1
   spark-submit --master=${SPARK_CLUSTER} run_query.py

4. Create your HPC config file with ``jade config hpc -c hpc_config.toml``. Set ``nodes`` in
   ``hpc_config.toml`` to be the number of compute nodes you want to participate in the Spark
   cluster. Spark will perform poorly if its scratch file space is on slow storage. You should
   specify requirements here that give you nodes with fast internal storage. On NREL's Eagle
   HPC cluster that is only the big-memory and GPU nodes. Alternatively, you can use nodes' tmpfs
   for scratch space or another directory altogether as described below.
5. If you want Spark to use GPUs, add GPU requirements to ``hpc_config.toml``. This example on Eagle
   will acquire nodes with two GPUs: ``gres=gpu:2``. Jade will detect this setting and add the
   appropriate Spark settings.
6. Run the command below to update the configuration with Spark parameters. Refer to ``--help`` for
   additional options. This will produce global spark configuration files in ``./spark/conf`` that you
   can customize. Refer to Spark documentation for help with the parameters.

   One parameter that you should customize is ``spark.sql.shuffle.partititons`` in
   ``spark/conf/spark-defaults.conf``.
   Jade sets them to the total number of cores in the cluster by default.  This
   `video <https://www.youtube.com/watch?v=daXEp4HmS-E&t=4251s>`_ offers an equation that works
   well: ``num_partitions = max_shufffle_write_size / target_partition_size``.

   You will have to run your job once to determine ``max_shuffle_write_size``. You can find it on
   the Spark UI ``Stages`` tab in the ``Shuffle Write`` column. Your ``target_partition_size``
   should be between 128 - 200 MB.

   The minimum ``partitions`` value should be the total number of cores in the cluster unless you
   want to leave some cores available for other jobs that may be running simultaneously.

   Note that you can also customize any of these settings in your script that calls ``spark-submit``
   or ``pyspark``.

::

    $ jade spark config -c <path-to-container> -h hpc_config.toml --update-config-file=config.json

7. If you set a custom memory requirement in your ``hpc_config.toml`` then Jade will increase the
   ``spark.executor.memory`` value in ``spark/conf/spark-defaults.conf``. The default value is
   intended to maximize memory for 7 executors on each compute node. Customize as needed.
8. View the changes to your ``config.json`` if desired.
9. If you are using compute nodes with slow internal storage, consider setting ``use_tmpfs_for_scratch``
   to true. Note that this reduces available worker memory by half and you'll need to adjust
   ``spark.executor.memory`` accordingly. You can set the option in ``config.json`` or by passing
   ``-U`` to the ``jade spark config`` command.
10. Similar to the previous point, you can specify ``alt_scratch`` to use your own scratch directory.
    On Eagle you may get decent performance if you specify a directory in ``/scratch/<username>``.
    Be sure to clean up this up periodically because Spark will write lots of data during shuffles.
11. Consider whether you want your jobs to be run inside or outside the container (default is outside).
   Jade will run each job inside the container if the ``run_user_script_inside_container`` option is
   set to true. You can set the option for each job in ``config.json`` or by passing ``-r`` to
   the ``jade spark config`` command. If you do run your script outside of the container, Jade will
   still set Spark environment variables to point to your local, customizable Spark config
   directory.
12. Set ``collect_worker_logs`` to true if your jobs are getting logs of errors. These can grow large.
13. Submit the jobs with ``jade submit-jobs config.json``. Jade will create a new cluster for each
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

3. ssh to the first compute node in your allocation.

4. If you want to use the custom Spark environment created by Jade, set the ``SPARK_CONF_DIR`` environment
variable so that your SparkSession gets initialized with the correct parameters.

This example assumes that your JADE output directory is ``./output`` and there is one job named ``1``.

.. code-block:: bash

   $ ssh <first-compute-node-name>
   $ cd <wherever-you-started-the-jade-jobs>
   $ export SPARK_CONF_DIR=./output/job-outputs/1/spark/conf


5. Run your code through ``pyspark`` or ``spark-submit``.

.. code-block:: bash

   $ pyspark --master=spark://`hostname`:7077


Run a Jupyter server
====================
This example shows how to make JADE start a Jupyter server with the environment ready to use the Spark
cluster.

1. Create a bash script with the content below. Save the script as ``start_notebook.sh``.

.. code-block:: bash

   #!/bin/bash
   unset XDG_RUNTIME_DIR
   export SPARK_CLUSTER=$1
   export PYSPARK_DRIVER_PYTHON=jupyter
   export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=0.0.0.0 --port 8889"
   pyspark --master=${SPARK_CLUSTER}
   echo "Spark cluster is running at ${SPARK_CLUSTER}" >&2
   echo "JADE output directory is ${2}" >&2
   sleep 10
   echo "Create an ssh tunnel with this command: ssh -L 8889:${HOSTNAME}:8889 -L 8080:${HOSTNAME}:8080 -L 4040:${HOSTNAME}:4040 ${USER}@el1.hpc.nrel.gov" >&2
   wait

2. Set the JADE command in ``commands.txt``/``config.json`` to be ``bash start_notebook.sh``.

3. Submit the jobs with ``jade submit-jobs config.json -o output``

4. Once the job is allocated run ``tail -f output/job-stdio/*.e``. After 15-20 seconds you will see console
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
   display(HTML("<style>.container { width:100% !important; }</style>"))
   spark = SparkSession.builder.appName("my_app").getOrCreate()

8. Connect to the Spark UI from your browser, if desired, to monitor your jobs.

http://localhost:4040 and/or http://localhost:8080

9. If you want to ensure that JADE shuts down the Spark cluster cleanly (preserving history)
   then you should shutdown the notebook. ssh to the first compute-node and run
   ``jupyter notebook stop 8889``.
 
Run a Jupyter notebook on an existing cluster
=============================================
Unlike the previous section, this example assumes that there is an existing cluster and you have
ssh'd into the master node.

1. Configure ``pyspark`` to create a Jupyter Notebook instead of a regular interactive session.

.. code-block:: bash

   $ export PYSPARK_DRIVER_PYTHON=jupyter
   $ export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=0.0.0.0 --port 8889"
   # If you have configured SPARK_HOME differently, don't run this command.
   $ export SPARK_HOME=`python -c "import pyspark;print(pyspark.__path__[0])"`

2. Start ``pyspark``, optionally with custom Spark parameters. It will create a Juypter
   notebook and print the connection information.

.. code-block:: bash

   $ pyspark

3. Create an ssh tunnel as described in the previous section.

4. Connect to the notebook from your computer's browser.

5. Connect to the ``SparkSession`` by pasting this code block into a cell.

.. code-block:: python

   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("my_app").getOrCreate()


Use nodes with Nvidia GPUs
==========================
If your compute nodes have Nvidia GPUs then you can leverage Nvidia's
`RAPIDS Accelerator for Apache Spark <https://nvidia.github.io/spark-rapids/>`_
to get substantially faster performance in some cases. Ensure that your compute nodes have all
required Nvidia software installed. This section assumes the presence of these files:

- /opt/sparkRapidsPlugin/cudf-22.04.0-cuda11.jar
- /opt/sparkRapidsPlugin/rapids-4-spark_2.12-22.04.0.jar

and these environment variables:

- export SPARK_RAPIDS_PLUGIN_JAR=/opt/sparkRapidsPlugin/rapids-4-spark_2.12-22.04.0.jar
- export SPARK_CUDF_JAR=/opt/sparkRapidsPlugin/cudf-22.04.0-cuda11.jar

Run a Spark job
---------------
This example works on NREL's Eagle HPC. It also assumes that you have ssh'd to the Spark master node.

If you want to run the job in your own environment outside of the container, copy the three files
mentioned above to your workspace and set the environment variables accordingly.

Refer to `Nvidia's tuning guide <https://nvidia.github.io/spark-rapids/docs/tuning-guide.html>`_.

.. code-block:: bash

   $ module load singularity-container
   $ singularity shell -B /scratch:/scratch -B /projects:/projects <path-to-continer>/nvidia_spark.sif
   $ pyspark --master spark://`hostname`:7077 \
     --name mysparkshell \
     --deploy-mode client  \
     --conf spark.executor.cores=4 \
     --conf spark.executor.instances=2 \
     --conf spark.executor.memory=25G \
     --conf spark.executor.memoryOverhead=3G \
     --conf spark.executor.resource.gpu.amount=1 \
     --conf spark.executor.resource.gpu.vendor=nvidia.com \
     --conf spark.locality.wait=0s \
     --conf spark.rapids.memory.pinnedPool.size=2G \
     --conf spark.rapids.sql.hasNans=false \
     --conf spark.rapids.sql.castFloatToString.enabled=true \
     --conf spark.rapids.sql.castStringToFloat.enabled=true \
     --conf spark.sql.files.maxPartitionBytes=512m \
     --conf spark.sql.shuffle.partitions=10 \
     --conf spark.task.cpus=1 \
     --conf spark.task.resource.gpu.amount=0.25 \
     --jars ${SPARK_CUDF_JAR},${SPARK_RAPIDS_PLUGIN_JAR} \
     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
     --driver-memory 25G

.. warning:: This example assumes that the dataframes do not contain NaN values.

.. note:: Add --conf spark.rapids.sql.explain=ALL to see whether jobs are running on the CPUs or GPUs.

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


Start a Spark Cluster on Arbitrary Compute Nodes
================================================
In some cases you may want to allocate compute nodes apart from Jade and then start a cluster. Similarly, you
may want to restart the cluster with different configuration settings and not have to relinquish compute
nodes. In the examples below Jade will stop all Spark processes on the nodes and then start a new cluster.

In this example Jade will start the cluster and then sleep indefinitely.

.. code-block:: bash

   $ jade start-spark-cluster --container <path-to-container> --spark-conf ./spark node1 node2 nodeN

The value passed to ``--spark-conf`` should be equal in format to the directory created above in ``jade spark config``.

In this example Jade will start the cluster and then run a user script to start a notebook. The script
must be executable.

.. code-block:: bash

   $ jade spark start-cluster --container <path-to-container> --spark-conf ./spark --script start_notebook.sh
