.. jade documentation master file, created by
   sphinx-quickstart on Mon May  6 14:12:42 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

JADE documentation
*******************

What is JADE?
=============
JADE stands for **Job Automation and Deployment Engine**.

JADE automates execution of simulations on any computer or HPC. The core
infrastructure is written in Python, but it allows for each individual job to
be any executable.

User Workflow
=============

#. Create Inputs and Configurations instances.
#. View available job parameters and add them to the configuration.
#. Submit the jobs for execution.
#. Post-process the results.

Extending JADE
===============
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


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   tutorial
   extensions
   jade
   design
   build_docs


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
