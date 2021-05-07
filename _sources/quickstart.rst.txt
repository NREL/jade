*****************
Quick Start Guide
*****************

This page provides short examples to get JADE up and running. It assumes that
you have completed installation.

Configure Jobs
==============
Create a text file with a list of commands, one per line.

.. code-block:: bash

    $ cat commands.txt
    bash my_script.sh ARG1
    bash my_script.sh ARG2

Create the JADE configuration.

.. code-block:: bash

    $ jade config create commands.txt
    Created configuration with 2 jobs.
    Dumped configuration to config.json.

Submit Jobs
===========

Local
-----
Run the jobs on the local system with this command. It will block until all
jobs complete.

.. code-block:: bash

    $ jade submit-jobs config.json --local -o output

HPC
---
Change to a directory on the shared filesystem (such as /scratch on Eagle).
JADE uses the filesystem for internal synchronization.

Configure your HPC account parameters with this command. Correct your account
name and other parameters as necessary.

.. code-block:: bash

    $ jade config hpc -a <account> -p short -t slurm -w "4:00:00" -c hpc_config.toml
    Created HPC config file hpc_config.toml

.. code-block:: bash

    $ jade submit-jobs config.json -h hpc_config.toml -o output

For more complicated configurations you'll likely want to give the parameters
some thought. You can run ``jade submit-jobs --help`` to see all available
options.

Job Status
===========
While jobs are running you can check status with this command:

.. code-block:: bash

    $ jade show-status -o output

The status is updated when each compute node starts or completes its execution
of a batch, so this status may not be current.


Job Results
===========
Once execution is complete you can view the results of the jobs.

.. code-block:: bash

    $ jade show-results -o output

