.. _simulation_implementations_label:

Simulation Implementations
##########################

Here are the current extensions in JADE. Refer to :ref:`extending_jade_label`
if you want to add a new extension.

.. _disco_pv_deployment_simulations_label:

DISCO PV Deployment Simulations
*******************************

- OpenDSS input data is automatically generated for all feeders and deployments
  based on DISCO PV Deployment schema.
- DISCO generates descriptors for each possible simulation job and allows the
  user to build batches of jobs.
- DISCO customizes input data based on user specification.
- JADE runs the simulations through PyDSS.

Refer to :ref:`disco_pv_deployment_simulation_details_label` for more
information.

JADE implementation classes
===========================

- :class:`~jade.pydss.pydss_inputs.PyDssInputs`
- :class:`~jade.distribution.deployment_parameters.DeploymentParameters`
- :class:`~jade.extensions.pydss_simulation.pydss_configuration.PyDssConfiguration`
- :class:`~jade.extensions.pydss_simulation.pydss_simulation.PyDssSimulation`


.. _user_defined_opendss_simulations_label:

User-Defined OpenDSS Simulations
********************************

- User creates OpenDSS input files for one or more simulations.
- User creates a JADE configuration for one or more simulation jobs.
- JADE runs the simulations through PyDSS.

JADE implementation classes
===========================

- :class:`~jade.pydss.user_scenario_pydss_inputs.UserScenarioPyDssInputs`
- :class:`~jade.pydss.jade.pydss.user_scenario_deployment_parameters.UserScenarioDeploymentParameters`
- :class:`~jade.pydss.user_scenario_pydss_configuration.UserScenarioPyDssConfiguration`
- :class:`~jade.pydss.user_scenario_pydss_simulation.UserScenarioPyDssSimulation`
