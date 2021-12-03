.. toctree::
   :hidden:

   Home page <self>
   API reference <_autosummary/jade>

******************
JADE documentation
******************

JADE stands for **Job Automation and Deployment Engine**.

JADE automates parallelized execution of jobs. It has specific support for
distributing work on HPC compute nodes but can also be executed locally.

The core infrastructure is written in Python, but it allows for each individual
job to be any executable.

Types of Jobs
=============
There are two ways to use JADE:

1. Execute a list of CLI commands.

- Put the commands in a text file, one per line.
- Create a JADE configuration with ``jade config create commands.txt``
- Run the jobs with ``jade submit-jobs config.json``

2. User-specific extension

JADE allows users to create extensions in order to customize configuration and
execution. Refer to :ref:`extending-jade-label` below.

Parallelization on HPC Compute Nodes
====================================
JADE offers the following parameters for customizing job execution on HPC:

- how many nodes to use
- how many jobs to run in parallel on each node (default is number of CPUs)
- how many jobs to run in a single node allocation (consider job duration and
  which HPC partition you are using)

Job Ordering
============
JADE's default behavior is to treat each job with equal priority. Dependencies
can be defined in the configuration to guarantee that a job won't start until
one or more other jobs complete.

Batch Pipeline
==============
JADE supports execution of a pipeline of job batches. The output of the first
batch of jobs can be piped to the next batch, and so on. This can be much
simpler and more efficient than defining a complex job-ordering scheme. Each
stage in the pipeline can also have independent parallelization parameters.

Refer to :ref:`batch_pipeline_label` for more information.

.. _extending-jade-label:

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

Refer to :ref:`advanced_guide_label` for more information.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   tutorial
   job_configuration
   submission_strategies
   pipeline
   spark_jobs
   singularity_containers
   distributed_submission
   advanced_usage

.. toctree::
   :maxdepth: 2
   :caption: Dev & Reference

   jade
   design
   build_docs

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
