Building the Documentation
##########################

Build locally
*************

::

    cd docs

    # Rebuild the API pages on first build or if code has changed.
    rm -rf source/jade
    sphinx-apidoc -o source/jade ../jade

    make html


Push to GitHub
**************

::

    cd docs
    make github
