**********************
Singularity Containers
**********************

If you are not familiar with Singularity containers please refer to
https://sylabs.io/guides/3.8/user-guide/introduction.html#why-use-singularityce

Here are some important takeaways:

1. HPC systems do not typically allow Docker containers because of security
   concerns. Running Docker requires root access but Singularity containers do
   not.

2. Singularity can convert Docker images to its own format, so you can create
   containers with Docker tools and base images, and then convert them for use
   on an HPC.

3. Containers are much more portable than environment modules.

Here is how you can use Singularity containers with JADE:

1. Wrap your Singularity commands in a bash script and make that script your
   JADE job.

Your users won't have to install your application and its dependencies but they
will still have to install JADE (and likely some Python virtual environment).

.. code-block:: bash

    $ cat wrapper-script.sh
    #!/bin/bash
    module load singularity-container
    singularity run path-to-container actual-job-script $1

    $ cat commands.txt
    bash wrapper-script.sh 1
    bash wrapper-script.sh 2
    bash wrapper-script.sh 3

    $ jade config create commands.txt

2. Package JADE with your application in a Singularity container. Assuming your
   HPC provides an installation of Singularity, your users won't have to
   install anything. Setting this up is more complex than #1 but it can be
   useful if you want to provide your users one thing to run.

   The rest of this page describes one way to do this.

Building a container that can be used for HPC submissions
=========================================================
There are plenty of instructions on the internet on how to build a Docker or
Singularity container and this page won't replicate them. This is one opinion
on a strategy.

1. Install Docker on your computer and create a container, hopefully starting
   from a base image that has most of what you want. Anyone will be able to use
   this image on a personal computer or an HPC.

2. Save the image to a tarfile and upload it to your HPC.

3. Convert the image to a writable Singularity container with a command like
   this:

.. code-block:: bash

    $ singularity build --sandbox my-container docker-archive://my-container.tar

4. Test the container in writable mode.

.. code-block:: bash

    $ singularity shell --writable my-container

5. Determine all of the required bind mounts. It's likely that commands for the
   HPC queueing system (i.e. SLURM) will not work. Here is a helpful `website
   <https://info.gwdg.de/wiki/doku.php?id=wiki:hpc:usage_of_slurm_within_a_singularity_container>`_.

   Here is a working example that you can put in your Dockerfile for use
   on NREL's HPC.

.. warning:: This sets ``$PATH`` on the Docker container but not Singularity.
   You still need to append to the path inside your Singularity container.

.. code-block:: docker

    RUN echo "export LD_LIBRARY_PATH=/usr/lib64:/nopt/slurm/current/lib64/slurm:$LD_LIBRARY_PATH" >> $HOME/.bashrc
    RUN echo "export PATH=$PATH:/nopt/slurm/current/bin" >> $HOME/.bashrc
    RUN echo "slurm:x:989:989:SLURM workload manager:/var/lib/slurm:/bin/bash" >> /etc/passwd
    RUN echo "slurm:x:989:" >> /etc/group


6. Whenever the HPC queueing system allocates a node and starts your process it
   will first try to change to the directory in which the submission occurred.
   So, it's important that this directory is a bind mount passed when you start
   the container. The recommended place to run jobs on NREL's HPC is
   /scratch/<username>, so /scratch should always be mounted there.


7. Adjust the appropriate settings in your compute node startup script:

.. code-block:: bash

    module load singularity-container
    export LD_LIBRARY_PATH=/usr/lib64:/nopt/slurm/current/lib64/slurm:$LD_LIBRARY_PATH
    export PATH=$PATH:/nopt/slurm/current/bin
    bash my-script.sh

8. Make sure that you can run your application, ``jade``, and the HPC
   executables.

9. Once everything is working, create a read-only image for your users wth a
   command like this:

.. code-block:: bash

    $ singularity build my-container.sif my-container

Note that you can skip step #4 if you already know the container will work.

.. code-block:: bash

    $ singularity build my-container.sif docker-archive://my-container.tar

Running a container that includes JADE
======================================

1. Change to the recommended HPC runtime directory (on NREL's HPC:
   ``/scratch/<username/``).

2. Load the Singularity module.

.. code-block:: bash

    module load singularity-container

3. Start your container. Be sure to bind-mount your current directory. Here is
   an NREL example:

.. code-block:: bash

    $ singularity shell -B /scratch:/scratch -B /projects:/projects \
        -B /nopt,/usr/lib64/libreadline.so.6,/usr/lib64/libhistory.so.6,/usr/lib64/libtinfo.so.5,/var/run/munge,/usr/lib64/libmunge.so.2,/run/munge \
        /scratch/<username>/my-container.sif

4. Add SLURM executables to the system path (this may go away in the future).

.. code-block:: bash

    export LD_LIBRARY_PATH=/usr/lib64:/nopt/slurm/current/lib64/slurm:$LD_LIBRARY_PATH"
    export PATH=$PATH:/nopt/slurm/current/bin"

5. Setup your JADE configuration to work with your container.

.. code-block:: bash

    $ jade config submitter-params -S -C <path-to-container> -c submitter_params.json

6. Edit ``submitter_params.json`` to ensure that all Singularity-parameters are
   correct and JADE can correctly start your container on each compute node.

7. Submit your JADE jobs from within the container.

.. code-block:: bash

    $ jade submit-jobs -s submitter-params.json config.json -o my-output-dir
