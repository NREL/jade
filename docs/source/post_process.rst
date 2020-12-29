************
Post-process
************

A post-process can be attached to an individual job, after the job run finishes,
then the post-process startes to run. For example, the post-process can be an
analysis program which could be used to run on top of a job outputs.


Define Post-process
===================

Suppose you have a package/module in script ``job.analysis.post_analysis.py``,

.. code-block:: python

    class PostAnalysis:
        """A class for your post-process"""

        def run(self, **kwargs):
            # TODO: implement your post-analysis logic

Where a ``run`` method is defined in the class.


Auto Config
===========

Define the post-process config file in ``.toml`` or ``.json``. For example,
the file ``~/my-post-analysis-config.toml`` is defined below,

.. code-block:: bash

    class = "PostAnalysis"
    module = "job.analysis.post_analysis"

    [data]
    name = "example"
    year = 2000
    count = 5
    # whatever the data needed to pass to the `run` method

To config the post-process with jobs, use the command below,

.. code-block:: bash

    $ jade auto-config --job-post-process-config-file ~/my-post-analysis-config.toml


Sumbit Jobs
===========

.. code-block:: bash

    $ jade submit-jobs config.json
