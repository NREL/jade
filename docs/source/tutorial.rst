Tutorial
########
This page describes how to use the JADE package to create, modify, and run
simulations locally or on an HPC.

Refer to :ref:`simulation_implementations_label` for descriptions of the
currently-supported simulation types.

If you are new to using Linux and IPython on the HPC, checkout this `intro
<https://github.nrel.gov/ehale/computing_resources/blob/master/linux-dev-workflow.md>`_.

Installation
************

JADE can be installed on computer or HPC. If trying to install it on your machine,
you can choose to install with/without docker.

Computer with docker
=====================


Docker can run on different OS platforms - Linux, Mac, Windows, etc.
Please follow the document https://docs.docker.com/ to install Docker CE
on your machine first. Then, can continue DISCO installation with docker.

1. Clone DISCO source code to your machine.

::

    git clone git@github.nrel.gov:Hosting-Capacity-Analysis/disco.git

2. Clone PyDSS source code to your ``disco`` folder.

::

    cd disco
    git clone https://github.nrel.gov/dthom/PyDSS.git
    cd PyDSS
    git checkout disco-integration

3. Build ``jade`` docker image

::

    docker build -t jade .

4. Run ``jade`` docker container

::

    docker run --rm -it -v absolute-model-inputs-path:/data/model-inputs jade

After the container starts, the terminal will show something like this

::

    (jade) root@d14851e20888:/data#

Then type ``jade`` to show JADE related commands

::

    (jade) root@d14851e20888:/data# jade
    Usage: jade [OPTIONS] COMMAND [ARGS]...

      Entry point

    Options:
      --help  Show this message and exit.

    Commands:
      auto-config                 Automatically create a configuration.
      auto-config-analysis        Automatically create a configuration.
      generate-input-data         Generate input data for a model.
      show-results                Shows the results of a batch of jobs.
      submit-jobs                 Submits jobs for execution, locally or on HPC.

This base image is https://hub.docker.com/r/continuumio/miniconda3, which is built
on top of ``debian``, so you can use Linux commands for operation.

5. To exit docker environment, just type

::

    exit

For more about docker commands, please refer https://docs.docker.com/engine/reference/commandline/docker/.

Computer or HPC without docker
==============================

1. Install JADE. Choose the default environment for running jobs or the
   development environment if you will be developing new code or documentation.

.. note:: The dev environment requires that pandoc and plantuml be installed.

   - Refer to `pandoc <https://pandoc.org/installing.html>`_.
   - plantuml on Mac: ``brew install plantuml``
   - plantuml on Linux: ``sudo apt-get install plantuml``
   - plantuml on Windows: `plantuml <http://plantuml.com/starting>`_.

::

    git clone git@github.nrel.gov:Hosting-Capacity-Analysis/disco.git
    cd disco
    # If conda is not already in your environment (such as on HPC):
    module load conda

    conda env create -f environment.yml
    # or
    conda env create -f dev-environment.yml

    conda activate jade
    pip install -e .

2. Install PyDSS. JADE currently requires an integration branch in a fork.

::

    cd ..
    git clone https://github.nrel.gov/dthom/PyDSS.git
    cd PyDSS
    git checkout disco-integration
    pip install -e .

3. Identify the input data to use. The current generated data is on Eagle at
/projects/la100/run1/distribution/model-inputs. If running locally then copy
this data.


Usage
*****

#. Build a simulation configuration from available input parameters in IPython.
#. Save the configuration to a TOML file and edit manually if needed.
#. Copy and modify the hpc_config.json file in the root of disco as necessary.
#. Submit the jobs for execution with the CLI tool ``jade submit-jobs``.
#. Post-process the results.

The recommended way to build a configuration is through IPython because of its
dynamic capabilities. The file init.py helps get you started with this process.
It initializes the environment with all required imports and sets up some
common variables.  Running the following command will execute all lines in the
script and leave you in an IPython interpreter::

    ipython -i ipython/init.py

This created the following variables to represent a small configuration:

- `inputs` of type :class:`~jade.extensions.pydss_simulation.pydss_inputs.PyDssInputs`
- `config` of type :class:`~jade.extensions.pydss_simulation.pydss_configuration.PyDssConfiguration`

In the interpreter type ``config<Enter>`` to display summaries.

Use these Python and IPython keystrokes to find out what you can do with these
objects (get documentation, tab through available methods)::

    config?<Enter>
    config.add_job?<Enter>
    config.<Tab>


Examples of common operations
=============================

Here are examples primarily for :ref:`disco_pv_deployment_simulations_label`.

Basic Python setup
------------------

.. code-block:: python

    import logging
    import os

    from jade.enums import ControlFlag
    from jade.loggers import setup_logging
    from jade.pydss.common import ConfigType
    from jade.extensions.pydss_simulation.pydss_configuration import PyDssConfiguration
    from jade.extensions.pydss_simulation.pydss_inputs import PyDssInputs
    from jade.extensions.pydss_simulation.pydss_simulation import PyDssSimulation

    logger = setup_logging("config", "log.txt", console_level=logging.INFO)

    inputs = PyDssInputs("model-inputs")

    # Auto-generate all possible jobs.
    config = PyDssConfiguration.auto_config(inputs)

    # Start with an empty config.
    config2 = PyDssConfiguration(inputs)

List available jobs
-------------------------

.. code-block:: python

    inputs.list_jobs()
    inputs.list_feeders()
    inputs.list_parameters(feeders="<your_feeder_name>")
    inputs.list_keys()
    inputs.list_control_flags()

Add a job
---------

.. code-block:: python

    jobs = inputs.list_jobs()
    config.add_job(jobs[0])

    # Or use a key.
    key = config.create_job_key("88_22", ControlFlag.PF_1, 1.15, 1.0, "deployment2.dss")
    job = inputs.get_job(key)
    config.add_job(job)

List configured jobs
---------------------------

.. code-block:: python

    jobs = config.list_jobs()
    config.show_jobs()

Remove a job
-------------------

.. code-block:: python

    config.remove_job(jobs[0])

Remove all jobs
---------------------------------------------------------------------

.. code-block:: python

    config.clear()

Save the configuration to a file.
---------------------------------

.. code-block:: python

    config.dump("config.json")

Build a configuration with all possible parameters.
---------------------------------------------------

.. code-block:: python

    inputs_path = os.path.join("..", "pydss-inputs-outputs")
    inputs = PyDssInputs(inputs_path)
    config = PyDssConfiguration.auto_config(inputs, PyDssSimulation)
    config.dump("config.json")


CLI Configuration
*****************
If you just want to create a configuration with all possible jobs then you
should use the auto-config CLI utility.

Example::

    jade auto-config model-inputs

Use the ``jq`` utility to view job summaries. Install with your package manager
or ``conda install -c conda-forge jq``.

Examples:

Get the number of jobs::

    jq '.["jobs"] | length' config.json

View job names::

    jq '.["jobs"] | .[].name' config.json

View feeder names (for :ref:`disco_pv_deployment_simulations_label` configs)::

    jq '.["jobs"] | .[].feeder' config.json


CLI Execution
*************
Jade provides a CLI utility to start simulations.

submit-jobs
===========
Start execution of jobs defined in a configuration file.  If executed on an HPC
this will submit the jobs to the HPC queue. Otherwise, it will run the jobs
locally.

It's important to understand how JADE submits HPC jobs in order to optimize
your performance.  JADE divides the jobs created by the user into batches.  It
makes one HPC node submission for each batch. Once running on a node it runs in
parallel a number of worker processes equal to the number of CPUs on that node
(36 on Eagle).

Parameters to keep in mind:

- **Number of jobs**: Number of jobs created by the user.
- **Max nodes**: Max number of job submissions (batches) to run in parallel.
- **Per-node_batch size**: Number of jobs to run on one node in one batch.
- **Allocation time**: How long it takes to acquire a node. Dependent on the
  HPC queue chosen and the priority given.
- **Average job runtime**: How long it takes a job to complete.

If the jobs are very quick to execute and it takes a long time to acquire a
node then you may be better off making per_node_batch_size higher and max_nodes
lower.

Conversely, if the jobs take a long time then you may want to do the opposite.

Run ``jade submit-jobs --help`` to see defaults.

Examples::

    # Use defaults.
    jade submit-jobs config.json

    # Specify options.
    jade submit-jobs config.json \
        --output=output-test
        --max-nodes=20
        --per-node-batch-size=500
        --hpc-config=hpc_config_test.toml
        --verbose

.. note::

   By default HPC nodes are requested at normal priority. Set qos=high in
   hpc_config.json to get faster allocations at twice the cost.


Customizing PyDSS parameters
****************************
PyDSS parameters likely need to be modified for your simulation. Here's how
to edit them in IPython.  Note that you can also modify them in a saved
configuration file.

.. code-block:: python

    # Assume a config object created above.
    from jade.pydss.common import ConfigType

    # View the parameters.
    for config_type in ConfigType:
        print(f"{config_type}: {config.get_pydss_config(config_type)}")

    # Read / modify / write.
    exports = config.get_pydss_config(ConfigType.EXPORT_BY_CLASS)
    exports["Lines"] = ["Currents", "Losses", "NormalAmps"]

    config.set_pydss_config(ConfigType.EXPORT_BY_CLASS, exports)
    config.dump("config.json")

Adding PyDSS controllers
========================

Generate a new config file with a customized controller. Afterwards, re-define
each job's ``pv_controllers`` field with the correct information in either
IPython or a text editor.

IPython example
---------------

.. code-block:: python

    # Create a new controller and add it to the base config.
    controller2 = config.get_default_pydss_controller_config()
    controller2["Control1"] = "CPF"
    config.add_pydss_controller_config("controller2", controller2)

    job = config.get_job("51_01__3__1.15__1.0__deployment241.dss")
    job.pv_controllers["default"] = ["262878_xfmr_1_2_3_pv", "262960_xfmr_1_2_pv"]
    job.pv_controllers["controller2"] = ["247443_xfmr_1_2_3_pv", "262921_xfmr_1_2_pv"]

    config.dump("config.json")

Text editor example
-------------------

.. code-block:: python

    # Create a new controller and add it to the base config.
    controller2 = config.get_default_pydss_controller_config()
    controller2["Control1"] = "CPF"
    config.add_pydss_controller_config("controller2", controller2)
    config.dump("config.json")

Open config.json in a text editor and make these changes to job parameter
section with the correct name.

**Original**::

    [[parameters]]
    ...
    name = "51_01__3__1.15__1.0__deployment241.dss"
    ...
    [parameters.pv_controllers]
    default = [ "247443_xfmr_1_2_3_pv", "262921_xfmr_1_2_pv", "262878_xfmr_1_2_3_pv", "262960_xfmr_1_2_pv",]

**New**::

    [[parameters]]
    ...
    name = "51_01__3__1.15__1.0__deployment241.dss"
    ...
    [parameters.pv_controllers]
    default = [ "262878_xfmr_1_2_3_pv", "262960_xfmr_1_2_pv",]
    controller2 = [ "247443_xfmr_1_2_3_pv", "262921_xfmr_1_2_pv"]


Adding PyDSS simulation configs
===============================

Here is an example of how to add and customize a simulation config.

.. warning:: This applies primarily to configurations for
   :ref:`user_defined_opendss_simulations_label`.  Job parameters defined for
   :ref:`disco_pv_deployment_simulations_label` have duplicate fields that will
   take precedence over this config.

.. code-block:: python

    # Create a new simulation config and add it to the base config.
    simulation2 = config.get_default_pydss_simulation_config()
    simulation2["Step resolution (sec)"] = 1800
    config.add_pydss_simulation_config("simulation2", simulation2)

    job = config.get_job("51_01__3__1.15__1.0__deployment241.dss")
    job.pydss_simulation_config = "simulation2"

    config.dump("config.json")

.. note:: As with the PyDSS controller case, you can make the same changes in a
   text editor.


Analysis
********
To get a quick summary of job results::

    jade show-results [--output OUTPUT-DIR]

Here is how to reload simulation objects to perform analysis.
This example is based on a PyDSS simulation. For generic analysis use
JobAnalysis instead of PyDssAnalysis.

.. code-block:: python

    import logging
    import os

    from jade.loggers import setup_logging
    from jade.pydss.pydss_analysis import PyDssAnalysis
    from jade.extensions.pydss_simulation.pydss_configuration import PyDssConfiguration

    logger = setup_logging("config", "log.txt", console_level=logging.INFO)

    config = PyDssConfiguration.deserialize("config.json")
    analysis = PyDssAnalysis("output", config)
    analysis.show_results()

    # Copy name from the output of show_results().
    name = analysis.list_results()[0].name

    # Look up job-specific parameters.
    job = analysis.get_job(name)
    print(job)
    print(job.deployment)
    print(job.deployment.metadata)

    simulation = analysis.get_simulation(name)

    # Read all dataframes into memory.
    results = analysis.read_results(simulation)
    print(results.keys())
    obj = results["CircuitTotalPower"]
    obj.df.head()

    # Alternatively, find out what result classes are available and create them
    # as needed (use less memory).
    analysis.list_available_result_classes(simulation)
    obj = CircuitTotalPower(results)
    obj.df.head()

    # Plot, if in Jupyter notebook.
    obj.plot()

    # Read events from the OpenDSS event log.
    event_log = analysis.read_dss_event_log(simulation)

    # Get the count of each capacitor's state changes from the event log.
    capacitor_changes = analysis.read_capacitor_changes(simulation)

To run a Jupyter notebook with data on Eagle you will need to connect to a
Data Analytics & Visualization (DAV) node or setup a reverse ssh tunnel.

This `page <https://www.nrel.gov/hpc/eagle-software-fastx.html>`_ describes how
to connect to a DAV node.


Simulation Analysis
===================
DISCO implements some jobs specifically to post-process data from simulations.
:class:`~jade.extensions.hosting_capacity_analysis.hosting_capacity_analysis.HostingCapacityAnalysis`
is an example.  Here is an example of how to use it.

Suppose you ran the simulation and output its data to the directory "output."

1. Create a config file with all feeders detected in the simulation.

::

    jade auto-config-analysis -c analysis-config.json output

2. Run the analysis jobs on all feeders.

::

    jade submit-jobs analysis-config.json -o analysis-output

3. Consume the output data.

.. code-block:: python

    from jade.utils.utils import load_data
    from jade.utils.postprocess import read_dataframe

    df_by_feeder = {}
    results = load_data("analysis-output/results.toml")
    for output in results["job_outputs"]:
        df = read_dataframe(output["data"], index_col="penetration")
        df_by_feeder[output["feeder"]] = df


User Scenario Workflow
**********************
This section describes a workflow for
:ref:`user_defined_opendss_simulations_label`.

Simulation Example
==================

.. code-block:: python

    import logging
    import os

    from jade.loggers import setup_logging
    from jade.extensions.user_scenario_pydss_simulation.user_scenario_pydss_inputs import UserScenarioPyDssInputs
    from jade.extensions.user_scenario_pydss_simulation.user_scenario_pydss_configuration import \
        UserScenarioDeploymentParameters, UserScenarioPyDssConfiguration
    from jade.extensions.user_scenario_pydss_simulation.user_scenario_pydss_simulation import  UserScenarioPyDssSimulation

    logger = setup_logging("config", "log.txt", console_level=logging.INFO)

    dss_directory = "dss-directory"
    dss_file = os.path.join(dss_directory, "Master_noVP.dss")
    scenario = "test"
    inputs = UserScenarioPyDssInputs(dss_directory)
    config = UserScenarioPyDssConfiguration(inputs)
    job = UserScenarioDeploymentParameters(scenario, dss_file)
    config.add_job(job)
    config.dump("config.json")

Now submit the job for execution::

    jade submit-jobs config.json

Debugging
*********
The first step is to identify the specific jobs that failed.

::

    jade show-results [--output OUTPUT-DIR]

Or inspect `output/results.toml`  in a text editor.

JADE generates multiple log files that can help debug failures.

- ``submit_jobs.log``: HPC-related information, such as the job ID and status
- ``run_jobs.log``: information about JADE and Dask starting and stopping
  jobs
- ``job_output_<HPC job ID>.e``: The HPC logs stdout and stderr from all
  processes to this file. Look here to debug unexpected crashes or hangs.

  - Python crashes will print ``Traceback`` to stderr, so that is a good string
    to search for.
  - Search for SLURM errors:  ``srun``, ``slurmstepd``, ``DUE TO TIME LIMIT``

::

    find output -name "*.log" -o -name "*.e"
    output/51_03__3__1.15__1.0__deployment52.dss/logs/deployment52.dss_simulation.log
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/Logs/pydss-project_deployment52.dss.log
    output/submit_jobs.log
    output/job_output_1151157.e

Useful grep commands::

    grep "WARNING\|ERROR" output/*log
    grep -n "srun\|slurmstepd\|Traceback" output/*.e


Debugging PyDSS Simulations
===========================
- ``<deployment_name>/logs/<deployment_name>_simulation.log``: Records all
  changes made to OpenDSS files as well as all PyDSS settings.
- ``<deployment_name>/pydss-project/Logs/pydss-project-<deployment_name>.log``:
  This log file is generated by PyDSS.

PyDSS Configuration
-------------------
JADE creates a PyDSS directory structure for each simulation in the output
directory. This contains the OpenDSS master file, PyDSS controller definitions,
export definitions, and output files.

::

    find output/51_03__3__1.15__1.0__deployment52.dss/pydss-project

    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/DSSfiles
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/DSSfiles/deployment.dss
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/Exports
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/Exports/deployment52.dss
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/Logs
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/Logs/pydss-project_deployment52.dss.log
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/PyDSS_Scenarios
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/PyDSS_Scenarios/deployment52.dss
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/PyDSS_Scenarios/deployment52.dss/ExportLists/exports.toml
    output/51_03__3__1.15__1.0__deployment52.dss/pydss-project/PyDSS_Scenarios/deployment52.dss/pyControllerList/controllers.toml
