
Append simulations.
*******************

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
Run simulation jobs with new parameters with an existing output directory.

Preconditions
=============
The output directory has existing results files and simulation outputs.

Postconditions
==============
Existing results files have the new results appended. If any parameters
overlapped between the runs then the older results will be overwritten.

Basic flow
==========
#. Same as :ref:`RunSimulations` use case except that the user supplies a
   parameter that instructs the software to append results.
