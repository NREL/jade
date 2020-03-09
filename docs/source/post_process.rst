
Post-process
############

Job Post-process
================

TODO:


Batch Post-process
==================

In JADE, we use extensions for batch post-processing. When ``auto-config`` your model inputs, 
please also specify the option ``--batch-post-process-extension`` for your batch post-processing purpose.
For example,

.. code-block:: bash

    jade auto-config \
    --job-post-process-config-file YOUR-JOB-CONFIG.TOML \
    --batch-post-process-extension YOUR-BATCH-EXTENSION \
    demo model-inputs

Behind the scene, please notice that

* ``BatchPostProcess`` would run the ``auto_config()`` method from extension's configuration class.
* The configuration class of your extension takes ``output`` from previous ``submit-jobs`` step as ``inputs``.
* The output of batch post-processing is under ``batch-post-process`` directory in your ``output``.
