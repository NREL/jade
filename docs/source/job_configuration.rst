******************
JADE Configuration
******************
This page describes the options available in a JADE configuration for generic
CLI commands (``config.json``). Most options also apply to other extensions.

Main Level
==========

- ``user_data``: Any global data that you would like to be available to jobs at
  runtime. Must be serializable in JSON format.
- ``submission_groups``: Optional array of :ref:`model_submission_group`
  objects.  Allows submitting jobs with differing batch sizes and HPC
  parameters. If set, each job must be assigned to a group.  Refer to
  :ref:`submission_group_behaviors` for additional information.
- ``jobs``: Array of job definitions. See below.


Per-Job Definition
==================
Each object in the array of ``jobs`` at the main level must be an instance
of GenericCommandParameters.

.. _model_generic_command_parameters:

GenericCommandParameters
------------------------

.. csv-table:: 
   :file: ../build/model_tables/GenericCommandParametersModel.csv
   :delim: tab

.. _submission_group_behaviors:

Submission Group Behaviors
==========================
JADE implements these behaviors when submission groups are defined.

JADE will raise an exception if the following parameters are not the same
across all submission groups:

- max_nodes
- poll_interval

JADE will override the following parameters in the submission groups with
parameters specified on the command line for ``jade submit-jobs``:

- dry_run
- generate_reports
- resource_monitor_interval
- verbose

JADE will override the following parameters in the submission groups with
parameters specified on the command line for ``jade submit-jobs`` **only**
if they are not set for the group:

- node_setup_script
- node_shutdown_script

The following parameters are completely controlled by the group:

- hpc_config
- num_processes
- per_node_batch_size
- time_based_batching
- try_add_blocked_jobs


.. toctree::
   :maxdepth: 2

   data_models
