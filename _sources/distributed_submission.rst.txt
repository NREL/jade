.. _distributed_submission_workflow:

*******************************
Distributed Submission Workflow
*******************************

This diagram illustrates the distributed nature of JADE's submission process.
Each compute node tries to promote itself to submitter to advance the workflow.
In the case where there are no blocked jobs and no pipeline this isn't
particularly interesting. The last compute node to finish its batch will
generate the final reports.  If there are blocked jobs or a pipeline with
multiple stages then a compute node will submit newly-unblocked jobs until the
entire process is complete.

At any point in the process the user can run ``jade show-status -o
<output-dir>`` to check the current status.

.. figure::  images/jade-submission-workflow.png
   :align:   center
   :scale: 100%

