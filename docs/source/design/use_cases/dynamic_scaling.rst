
Dynamic scaling
***************

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
Software automatically scales HPC nodes based on availability and jobs.

Basic flow
==========
#. User starts jobs.
#. Software automatically determines the number of nodes to request based on
   the number of jobs.
#. If the requested number of nodes aren't available then the software starts
   jobs on as many nodes as are available.
#. As nodes become available, the software acquires them and starts jobs on
   them.
