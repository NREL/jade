
**********
Spark Jobs
**********

JADE has functionality to create an ephemeral Spark cluster on HPC compute nodes and then run a
job on that cluster.

**Prerequisite**: The Spark software must be installed inside a Singularity container.

Setup
=====
1. Create a Docker container with Spark installed. Here is an `example
   <https://github.com/dsgrid/dsgrid/blob/main/Dockerfile>`_.
2. Convert that container to Singularity and copy the image to the HPC's shared file system.
3. Follow the normal steps to create your Jade configuration, such as with ``jade config create
   commands.txt``. The commands should be the scripts that you want to run against the Spark
   cluster including any command-line arguments. **Note**: Jade will append two positional
   arguments to your command line arguments: the spark cluster (spark://node_name:7077) and the
   job output directory.
4. Create your HPC config file with ``jade config hpc -c hpc_config.toml``. Set ``nodes`` in
   ``hpc_config.toml`` to be the number of compute nodes you want to participate in the Spark
   cluster.
5. Run this command to update the configuration with Spark parameters. Refer to ``--help`` for
   additional options. This will produce spark configuration files in ``./spark/conf`` that you
   can customize.  One possible customization is ``spark.sql.shuffle.partititons`` and 
   ``spark.default.parallelism`` in ``spark/conf/spark-defaults.conf``. Jade sets them to the total
   number of cores in the cluster by default. Refer to Spark documentation for help with the
   parameters.

::

    $ jade config spark -c <path-to-container> -h hpc_config.toml --update-config-file=config.json

5. View the changes to your ``config.json`` if desired.
6. Submit the jobs with ``jade submit-jobs config.json``. Jade will create a new cluster for each
   job, running them sequentially.

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
