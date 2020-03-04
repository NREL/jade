
Post-process
############

Job Post-process
================

TODO:


Batch Post-process
==================

.. code-block:: python

    from jade.jobs.batch_post_process import AbstractBatchPostProcess

    class AggregationPostProcess(AbstractBatchPostProcess):

        def run(self, data, output):
            print(data)
            print(output)

Please keep the positional arguments ``data`` and ``output``, where ``data`` includes all results
information from job-based post-process, you should parse the ``data`` information and do further
processing based on your needs. And, ``output`` is the directory where the aggregated result
should go.
