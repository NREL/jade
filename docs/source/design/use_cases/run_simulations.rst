
.. _RunSimulations:

Run simulations.
****************

Primary Actor
=============
Power Systems engineer

Scope
=====
User execution

Level
=====
User

Brief
=====
Run a batch of simulation jobs locally or on HPC.

Preconditions
=============
The output directory does not have files that will be overwritten.

Postconditions
==============
By default the software will exit with an error if any existing results files
will be overwritten.

Basic flow
==========
#. User starts IPython session, loads configuration file or creates new one.
#. Enter commands to submit the jobs for execution.
#. Alternatively, submit the jobs with a command-line (CLI) utility.
#. Software provides an option to run sequentially on HPC in order to simplify
   debug process.
#. The software reports a link to the Dask dashboard.
#. Monitor progress in IPython or connect to the Dask dashboard.
#. Software automatically performs post-processing and reports results.
