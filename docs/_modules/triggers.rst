Real-time triggering
====================

Introduction
------------
The ARTS GPU cluster receives both a Stokes I and Stokes IQUV data stream from the
beamformer, and searches the Stokes I data for transients in real-time using a GPU pipeline,
`AMBER <https://www.github.com/AA-ALERT/AMBER>`_. The real-time triggering part of DARC takes
the candidate metadata produced by AMBER, and decides whether or not to send a trigger to store
IQUV data, observe with the LOFAR transient buffer boards (TBBs), or send a VOEvent.

Reading AMBER candidates
^^^^^^^^^^^^^^^^^^^^^^^^
Each worker node runs three instances of AMBER, each searching a different part of parameter space.
The candidate metadata is written to a text file, one file per AMBER instance. These files are read by
the :class:`AMBERListener <darc.amber_listener.AMBERListener>` module, which reads the files in a
way similar to the ``tail -f`` command in bash. Each line of an AMBER file, containing the metadata of one candidate,
is fed to the next modules as-is, i.e. as one string. As the triggers with different DMs and the same arrival time
are detected by AMBER at different times, the candidates as output by
:class:`AMBERListener <darc.amber_listener.AMBERListener>` are not necessarily in chronological order.

Clustering and thresholding
^^^^^^^^^^^^^^^^^^^^^^^^^^^

IQUV triggering
^^^^^^^^^^^^^^^

LOFAR triggering
^^^^^^^^^^^^^^^^

Sending VOEvents
^^^^^^^^^^^^^^^^
