Welcome to DARC's documentation
===============================
DARC (Data Analysis of Real-time Candidates) is the real-time processing pipeline of the
Apertif Radio Transient System (ARTS). DARC consists of several services, that each execute
a subset of the pipeline. The real-time part analyses trigger metadata and determines whether
to send an IQUV trigger, LOFAR trigger and/or VOEvent. The offline part extracts the filterbank data
of each candidate, uses a neural network to classify them and sends a summary of each observation
to the astronomers. The offline part can run completely offline, i.e. only starting after the data recording
of an observation finishes, or in semi-real-time, where every candidate is processed in chronological order
as soon as they come in.

See one of the following links for more information:

.. toctree::
    :maxdepth: 1

    _modules/triggers
    _modules/processor
    _modules/offline
    _modules/api



Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
