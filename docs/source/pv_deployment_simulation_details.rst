
.. _disco_pv_deployment_simulation_details_label:

DISCO PV Deployment Simulation Details
######################################
The DISCO module implements the required JADE extension classes to run
customized power flow simulations and analysis.

Generate OpenDSS Inputs
***********************
This process reads required data from database X, generates OpenDSS
input files, stores them in a filesystem structure on Eagle, and then generates
a master descriptor file (JSON) that defines each possible simulation
configuration.

Descriptor Schema
=================
The master descriptor file describes all feeders and their possible
deployments.

Rules:

- If a control_flag is 3 (Volt/VAR) then a Volt/VAR curve must be provided.
  The name must match a value in :class:`~jade.enums.VoltVarCurve`
- start_time and end_time must be set with timestamps.
- If is_qsts == false (snapshot analysis) then start_time and end_time must be the same.
- control_flags, dc_ac_ratios, kva_to_kw_ratings, and volt_var_curves may be
  empty arrays to represent no-PV scenarios.

This example JSON show the required fields and values for each feeder and
deployment.

.. code-block:: json

    {
      "feeders": [
        {
          "deployments": [
            {
              "control_flags": [3],
              "dc_ac_ratios": [1.15],
              "kva_to_kw_ratings": [1],
              "loadshape_file": null,
              "loadshape_location": null,
              "name": "deployment1",
              "penetration": 20,
              "placement_type": "close",
              "pv_location": "/projects/X/feeder_path/PVSystems.dss",
              "sample": 9,
              "volt_var_curves": ["curveX"]
            }
          ],
          "end_time": "2012-07-20_15:00:00.000",
          "is_qsts": false,
          "loadshape_location": null,
          "name": "51_13",
          "opendss_location": "/projects/X/path",
          "start_time": "2012-07-20_15:00:00.000",
          "step_resolution": 900
        }
      ],
      "type": "OpenDSS"
    }


.. todo:: Details on input data source:  Tarek, Kwami
.. todo:: Details on script source code:  Tarek, Kwami


Generate DISCO simulation job descriptors
*****************************************
DISCO generates one descriptor file (JSON) for each permutation of possible
simulation jobs in a directory on the HPC filesystem.

Here are the steps involved:

#. Read the master descriptor file, described above.
#. Validate each feeder and deployment descriptor.
#. Create a directory structure for the OpenDSS files.
#. Copy all source OpenDSS files to the target directories. This includes
   definitions of all systems and devices as well as load shapes, if
   applicable.
#. If applicable, copy the PV deployment to the target directory and add a
   redirect to the master file.
#. If requested by the user, comment-out the redirect to the existing PV
   deployment file within the master file. This is an optional command-line
   parameter.
#. Set all files read-only.

.. todo:: In the base case where there is no PV, how will PyDSS get the master file instead of the PV file?

.. todo:: When is a user going to set --exclude-pv-systems?


Descriptor Schema
=================
This example schema shows one job descriptor.

.. code-block:: json

    {
      "deployment": {
        "control_flag": 3,
        "dc_ac_ratio": 1.15,
        "directory": "/lustre/eaglefs/projects/X/model-inputs",
        "kva_to_kw_rating": 1,
        "metadata": {
          "penetration": 45,
          "placement_type": "far",
          "sample": 4
        },
        "name": "deployment100.dss",
        "pv_locations": [
          "121607_xfmr_1_2_pv"
        ],
        "volt_var_curve": "curveX"
      },
      "feeder": "123_01",
      "simulation": {
        "end_time": "2012-07-20_15:00:00.000",
        "is_qsts": false,
        "start_time": "2012-07-20_15:00:00.000",
        "step_resolution": 900
      }
    }

Source Code
===========
This script is contained with DISCO. It can be executed with this command::

    jade generate-input-data [--exclude-pv-systems] [--output DIR] CONFIG_FILE

Refer to :class:`~jade.data_creation.open_dss_generator` for more details.


Creating DISCO simulation jobs
******************************
JADE reads all deployment descriptor files and presents them to the user to
filter and select. The user adds all desired deployments to a configuration.

If a deployment descriptor specifies a Volt/VAR curve then DISCO will
automatically configure the corresponding PyDSS PVController object for each
new PVSystem in that deployment. 

The user can customize any of the PyDSS settings in the DISCO configuratino
file before submitting the jobs for execution. 


DISCO Execution
***************
Before starting the simulation DISCO re-calculates the kVA and pctPmpp values
in the .dss files based on the job's DC/AC ratio, kVA-kW rating, and irradiance
scaling factor.
