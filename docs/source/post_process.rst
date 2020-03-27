
Post-process
############

Job Post-process
================

TODO:


Batch Post-process
==================

In JADE, we use generic commands for batch post-processing. When ``auto-config`` your model inputs in ``jade``, 
please specify the option ``--batch-post-process-config-file`` for batch post-processing.

Let's take extension ``demo`` for example, in this extension, we perform auto-regression analysis for 
country's ``gdp`` values. In each job (or country), it take a CSV file which contains ``gdp`` values, 
and generate a new CSV file ``result.csv`` containing ``pred_gdp`` values.

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

Our batch task is to collect ``result.csv`` files from all jobs, extract ``pred_gdp`` column from 
each ``result.csv`` file, and aggregate them in one CSV file. Here, we have a script 
``jade/extensions/demo/merge_pre_gdp.py`` for this batch post-processing, the exptected output is ``pred_gdp.csv``.

How to run the jobs with a bach post-process? Please follow steps below:

**1. Develop Batch Post-process Script**

Here, we use this script ``jade/extensions/demo/merge_pred_gdp.py`` for batch post-processing,
please refer to this file for details.

**2. Test the Script in Command**

We run the this command for batch post-processing,

.. code-block:: bash

    python jade/extensions/demo/merge_pred_gdp.py run output

**3. Put the Commands in a File**

Create a text file, let's say ``batch-post-process.txt``, which contains the command above.

**4. Config Batch Post-process via JADE**

Generate ``config.json`` with option ``--batch-post-process-config-file``, like this:

.. code-block:: bash

    jade auto-config --batch-post-process-config-file batch-post-process.text demo test/data/demo

In the generated ``config.json`` file, there's a attribute ``batch_post_process_config`` as below:

.. code-block :: python

    {
        "class": "AutoRegressionConfiguration",
        "extension": "demo",
        "jobs_directory": null,
        "batch_post_process_config": {
            "type": "Commands",
            "file": "batch-post-process.txt"
        },
        "jobs": [
            {
            "country": "australia",
            "data": "jade/tests/data/demo/gdp/countries/australia.csv"
            },
            {
            "country": "brazil",
            "data": "jade/tests/data/demo/gdp/countries/brazil.csv"
            },
            {
            "country": "united_states",
            "data": "jade/tests/data/demo/gdp/countries/united_states.csv"
            }
        ]
    }

**5. Submit Jobs with Batch Post-process Config**

Finally, we submit jobs via ``jade``, use the command below:

.. code-block:: bash

    jade submit-jobs config.json

Let's take a look at the ``output`` directory, you'll notice that ``batch-post-process`` results were generated.

.. code-block:: bash

    $ tree output
    output
    ├── batch-post-process
    │   ├── config.json
    │   ├── diff.patch
    │   ├── job-outputs
    │   ├── pred_gdp.csv
    │   └── results.json
    ...
    ├── post-config.json
    ...

    6 directories, 24 files

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
