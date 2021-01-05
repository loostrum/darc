Welcome to DARC's documentation
===============================
DARC (Data Analysis of Real-time Candidates) is the real-time processing pipeline of the
Apertif Radio Transient System (ARTS). DARC consists of several services, that each execute
a subset of the pipeline. The pipeline consists of four global parts:

#. Real-time triggering based on metadata of candidate transients
#. Real-time data processing of candidate transients
#. Offline post-processing
#. Monitoring & control

For more information about these different parts of DARC, consult one of the following pages:

.. toctree::
    :maxdepth: 1

    _modules/triggers
    _modules/processor
    _modules/offline
    _modules/mac
    _modules/api
