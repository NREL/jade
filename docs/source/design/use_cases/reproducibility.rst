
Post-process results.
*********************

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
User needs to reproduce earlier results or look at the exact source code that
generated the results.

Preconditions
=============
Simulations have completed successfully and generated results files.

Basic flow
==========
#. At startup the software records information from git in results directory:
   branch, commit, diffs of modified files.
#. Software provides an input option to revert all source files to the earlier
   version.
#. User restarts the simulations.
