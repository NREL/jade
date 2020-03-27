.. _batch_pipeline_label:

Batch Pipeline
==============
After all jobs in a batch complete you may want to run additional code to
process the results. You can use the ``jade pipeline`` command for this
purpose.

JADE will run ``submit-jobs`` on a series of config files sequentially. It will
pipe the output directory of each stage to the next stage.

To create the pipeline the user must provide a list of scripts that will be
used to create the JADE config file for each stage.

Create the pipeline
-------------------
::

    jade pipeline create batch1-auto-config.sh batch2-auto-config.sh -c pipeline.toml

Customize the config
--------------------
``pipeline.toml`` will have default values for each ``jade submit-jobs``
command. You may may want to override the max-nodes or per-node-batch-size
parameters for each stage.

Submit the pipeline
-------------------
::

    jade pipeline submit pipeline.toml


Example
-------
Let's use the extension ``demo`` as an example. This extension performs
auto-regression analysis for the ``gdp`` values for several countries. In each
job (or country), it reads a CSV file containing ``gdp`` values, and generates
a new CSV file ``result.csv`` containing ``pred_gdp`` values.

Suppose that we want to merge each job's output file into one file once all
jobs are complete.

The first step is to write a script to produce the summary file. Here's how to
to run the demo extension on test data.

.. code-block:: bash

    $ jade auto-config demo tests/data/demo -c config.json
    $ jade submit-jobs config.json
    $ tree output
    output
    ├── config.json
    ├── diff.patch
    ├── job-outputs
    │   ├── australia
    │   │   ├── events.log
    │   │   ├── result.csv
    │   │   ├── result.png
    │   │   ├── run.log
    │   │   └── summary.toml
    │   ├── brazil
    │   │   ├── events.log
    │   │   ├── result.csv
    │   │   ├── result.png
    │   │   ├── run.log
    │   │   └── summary.toml
    │   └── united_states
    │       ├── events.log
    │       ├── result.csv
    │       ├── result.png
    │       ├── run.log
    │       └── summary.toml
    ├── results.json
    └── submit_jobs.log

.. note::

    Please note that, we use datasets ``tests/data/gdp`` which contains only 3 countries.

The content of ``result.csv`` looks similar this,

.. code-block:: bash

    year,gdp,pred_gdp
    1960,543300000000,
    1961,563300000000,
    1962,605100000000,
    ...
    2016,18707188235000,19406250376876.492
    2017,19485393853000,20519007253667.656
    2018,20494100000000,20672861935684.523

Our post-processing task is to collect ``result.csv`` files from all jobs, extract ``pred_gdp`` column from 
each ``result.csv`` file, and aggregate them in one CSV file. The script
``jade/extensions/demo/merge_pre_gdp.py`` writes this result to ``pred_gdp.csv``.


Now let's automate this workflow in a JADE pipeline using two stages.

The first stage will use the ``demo`` extension. The script ``jade/extensions/demo/create_demo_config.sh``
creates its config file.

.. code-block:: bash

    $ cat jade/extensions/demo/create_demo_config.sh
    #!/bin/bash
    jade auto-config demo tests/data/demo -c config-stage1.json

The second stage will use the ``generic_command`` extension. We will create a
config that runs one "generic_command" - the script above to post-process the
results.

Note that this script receives a CLI argument that needs to be the output
directory of the first stage.

.. code-block:: bash

    cat ./jade/extensions/demo/create_merge_pred_gdp.sh                                            
    #!/bin/bash
    if [ -z $1 ]; then
        echo "Usage: create_merge_pred_gdp.sh AUTO_REGRESSION_OUTPUT"
        exit 1
    fi
    echo "python jade/extensions/demo/merge_pred_gdp.py run $1" > pred_gdp_commands.txt
    jade auto-config generic_command pred_gdp_commands.txt -c config-stage2.json

Let's create the pipeline and submit it for execution.

.. code-block:: bash

    $ jade pipeline create ./jade/extensions/demo/create_demo_config.sh ./jade/extensions/demo/create_merge_pred_gdp.sh  
    Created pipeline config file pipeline.toml

    $ jade pipeline submit pipeline.toml

Let's take a look at the ``output`` directory. You'll notice that ``batch-post-process`` results were generated.
Note that this script wrote the summary results into the stage1 results. It could have recorded them in
stage2.

.. code-block:: bash

    tree output              
    output
    ├── config-stage1.json
    ├── config-stage2.json
    ├── output-stage1
    │   ├── batch-post-process
    │   │   └── pred_gdp.csv
    │   ├── config.json
    │   ├── job-outputs
    │   │   ├── australia
    │   │   │   ├── events.log
    │   │   │   ├── result.csv
    │   │   │   ├── result.png
    │   │   │   ├── run.log
    │   │   │   └── summary.toml
    │   │   ├── brazil
    │   │   │   ├── events.log
    │   │   │   ├── result.csv
    │   │   │   ├── result.png
    │   │   │   ├── run.log
    │   │   │   └── summary.toml
    │   │   └── united_states
    │   │       ├── events.log
    │   │       ├── result.csv
    │   │       ├── result.png
    │   │       ├── run.log
    │   │       └── summary.toml
    │   ├── results.json
    │   └── submit_jobs.log
    ├── output-stage2
    │   ├── config.json
    │   ├── job-outputs
    │   ├── results.json
    │   └── submit_jobs.log
    └── pipeline_submit.log

In ``pred_gdp.csv``, you'll see the content:

.. code-block:: bash

    year,brazil,australia,united_states
    1960,,,
    1961,,,
    1962,,,
    ...
    2016,2080587377798.5112,1258003336600.582,19406250376876.49
    2017,1827457759144.0063,1438897367269.8796,20519007253667.656
    2018,1995335978627.933,2154574393156.4248,20672861935684.523


Done!
