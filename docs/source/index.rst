.. jade documentation master file, created by
   sphinx-quickstart on Mon May  6 14:12:42 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

JADE documentation
*******************

What is JADE?
=============
JADE stands for **Job Automation and Deployment Engine**.

JADE automates execution of jobs on any computer or HPC. The core
infrastructure is written in Python, but it allows for each individual job to
be any executable.

Types of Jobs
=============
There are two ways to use JADE:

1. Generic commands

You want to submit a list of CLI commands to HPC nodes in parallel. You will
need to configure the following within JADE:

- how many nodes to use
- how many jobs to run in parallel on each node
- how many jobs to run in a single node allocation (consider job duration and
  which HPC partition you are using)
- dependencies between jobs (i.e., job ID 4 cannot start until job ID 10
  finishes)

You can then submit the jobs from either a login node or compute node.

Refer to :ref:`generic_command_extension_label` for more information.

2. User-specific extension

You want develop your own customized JADE extension. Refer to the next section.

Batch Pipeline
==============
JADE supports execution of a pipeline of job batches. The output of the first
batch of jobs can be piped to the next batch, and so on.

Refer to :ref:`batch_pipeline_label` for more information.

Extending JADE
==============
JADE can be extended to support any type of job. Re-use or implement derived
classes from the following:

- :class:`~jade.jobs.job_parameters_interface.JobParametersInterface`:
  Defines the attributes of a job.
- :class:`~jade.jobs.job_inputs_interface.JobInputsInterface`:
  Defines the available input parameters for all jobs.
- :class:`~jade.jobs.job_configuration.JobConfiguration`:
  Defines a batch of jobs to be executed. 
- :class:`~jade.jobs.job_execution_interface.JobExecutionInterface`:
  Code to run a job.

Refer to :ref:`extensions_label` for more information.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tutorial
   extensions
   pipeline
   post_process
   jade
   design
   build_docs


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
