
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
   arguments to your command line arguments: the spark cluster (spark://<node_name>:7077) and the
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
