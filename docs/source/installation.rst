************
Installation
************

JADE can be installed on your computer or HPC. If trying to install it on your
computer, you can choose to install it in a conda environment or Docker
container.

Computer or HPC with conda
==========================
1. Clone JADE.

.. code-block:: bash

    $ git clone git@github.com:NREL/jade.git
    $ cd jade

2. Choose a virtual environment in which to install JADE.  This can be an
   existing `conda <https://docs.conda.io/en/latest/miniconda.html>`_
   environment or an environment from something like `pyenv
   <https://github.com/pyenv/pyenv>`_.  A validated conda environment is
   provided in the JADE repository. 

.. code-block:: bash

    $ conda env create -f environment.yml -n jade
    $ conda activate jade

3. Install JADE. 

.. code-block:: bash

    $ pip install -e .

    # If you will also be developing JADE code then include dev packages.
    $ pip install -e . -r dev-requirements.txt

.. note:: The dev packages require that pandoc and plantuml be installed.

   - Refer to `pandoc <https://pandoc.org/installing.html>`_.
   - plantuml on Mac: ``brew install plantuml``
   - plantuml on Linux: ``sudo apt-get install plantuml``
   - plantuml on Windows: `plantuml <http://plantuml.com/starting>`_.


Computer with docker
=====================
Docker can run on different OS platforms - Linux, Mac, Windows, etc.
Please follow the document https://docs.docker.com/ to install Docker CE
on your machine first. Then you can continue JADE installation with docker.

1. Clone JADE source code to your machine.

.. code-block:: bash

    $ git clone git@github.com:NREL/jade.git

2. Build ``jade`` docker image

.. code-block:: bash

    $ docker build -t jade .

3. Run ``jade`` docker container

.. code-block:: bash

    $ docker run --rm -it -v absolute-input-data-path:/data jade

After the container starts, the terminal will show something like this

.. code-block:: bash

    (jade) root@d14851e20888:/data#

Then type ``jade`` to show JADE related commands

.. code-block:: bash

    (jade) root@d14851e20888:/data# jade

    Usage: jade [OPTIONS] COMMAND [ARGS]...

      JADE commands

    Options:
      --help  Show this message and exit.

    Commands:
      auto-config   Automatically create a configuration.
      config        Manage a JADE configuration.
      extensions    Manage JADE extensions.
      pipeline      Manage JADE execution pipeline.
      show-events   Shows the events after jobs run.
      show-results  Shows the results of a batch of jobs.
      stats         View stats from a run.
      submit-jobs   Submits jobs for execution, locally or on HPC.

This base image is https://hub.docker.com/r/continuumio/miniconda3, which is
built on top of ``debian``, so you can use Linux commands for operation.

4. To exit docker environment, just type

.. code-block:: bash

    $ exit

For more about docker commands, please refer https://docs.docker.com/engine/reference/commandline/docker/.
