
Restart failed simulations.
***************************

Primary Actor
=============
Power Systems engineer

Scope
=====
User execution

Level
=====
User

Brief
=====
User sees that a subset of simulations failed because of a software bug or
intermittent problem and wants to automatically restart them.

Preconditions
=============
If the simulations were initiated from an IPython session, that session is
still active.

Basic flow
==========
#. User enters command to restart failed jobs. Software automatically detects
   which simulations to re-run based on the results file.
#. Alternatively, user initiates the restart from IPython or a CLI utility by
   passing the path to the results file.
#. Software restarts the jobs and runs to completion as desribed in "Run
   simulations."
