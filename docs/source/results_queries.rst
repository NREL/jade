.. _results_queries:

***************
Results Queries
***************
JADE stores job results, events, and summaries of resource utilization stats (if enabled) in a
SQLite database at ``<output-dir>/results.db``.

.. note:: This feature was added in v0.9.4. If you ran your jobs with an earlier version,
   you can still generate this file with ``jade db ingest-results <output-dir>``

The information in the database is redundant with information in other files, such as
``<output-dir>/results.txt``. However, using the database can make it easier to filter and join
tables. For example, you may want to include job runtimes, HPC job IDs, and per-process CPU and
memory stats in the same table.

Below are some example queries. All examples assume that you start sqlite3 with this command:

.. code-block:: bash

   $ sqlite3 -header -column <output-dir>/results.db

Show available tables
=====================
.. code-block:: bash

    sqlite> .tables

View job results
================

Show all results
----------------
.. code-block:: bash

    sqlite> SELECT * FROM result;

Show only failed results
------------------------
.. code-block:: bash

    sqlite> SELECT * FROM result WHERE return_code != 0;

Show only missing/timed-out jobs
--------------------------------
.. code-block:: bash

    sqlite> SELECT * FROM result WHERE status = 'missing';

View all CPU and memory usage by each job
=============================================

.. note:: This requires that you enabled 'process' in 'resource_monitor_stats' in the submission
   parameters.

.. code-block:: bash

    sqlite> SELECT * from process; 

View maximum CPU and memory usage by each job
=============================================

.. code-block:: bash

    sqlite> SELECT 
        name
        ,cpu_percent AS max_cpu_percent
        ,rss / (1024*1024*1024) AS max_mem_gb
        FROM process
        WHERE stat = 'maximum'
        ORDER BY name;

View successful job runtimes in hours along with per-job resource utilization
=============================================================================

.. code-block:: bash

    sqlite> SELECT 
        result.name
        ,exec_time_s / (60*60) AS exec_time_h
        ,hpc_job_id
        ,process.cpu_percent AS max_cpu_percent
        ,process.rss / (1024*1024*1024) AS max_mem_gb
        ,bytes_consumed.bytes_consumed
        FROM result
        JOIN process
        ON result.name = process.name
        JOIN bytes_consumed
        ON result.name = bytes_consumed.source
        WHERE
            result.return_code = 0
            AND process.stat = 'maximum'
        ORDER BY result.name;

.. note:: ``bytes_consumed`` is only valid if you configure your jobs to use the JADE runtime
   output directory.
