**************************
Building the Documentation
**************************

Build locally
=============

.. code-block:: bash

    $ cd docs

    # Rebuild the API pages on first build or if code has changed.
    $ rm -rf source/jade
    $ sphinx-apidoc -o source/jade ../jade

    $ make html


Push to GitHub
==============

.. code-block:: bash

    $ cd docs
    $ make github
