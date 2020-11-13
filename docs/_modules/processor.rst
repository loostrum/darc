Real-time data processing
=========================

Introduction
------------
The ARTS transient search pipeline, `AMBER <https://www.github.com/AA-ALERT/AMBER>`_, produces a file with
metadata of each candidate. The goal of the real-time processing part of DARC is to extract the data of these candidates,
determine whether or not there are likely to be real, visualize the good candidates, and send an overview of each observation
including figures of the candidates to the astronomers.

Similar to the :ref:`real-time triggering <_modules/triggers:Real-time triggering>` part of DARC, each worker node of the GPU cluster
can operate mostly independently. The master node takes care of gathering the figures and summaries from the different nodes,
and sends the results to the astronomers. On this page, each step of the real-time data processing system is explained in more detail.

Observation management
----------------------
Each observation is managed by one instance of :class:`Processor <darc.processor.Processor>` on the worker nodes,
and :class:`ProcessorMaster <darc.processor_master.ProcessorMaster>` on the master node.

It is possible that a new observation is started before the previous observation has finished processing. Therefore,
several instances of the processor must be able to run simultaneously. This is managed by
:class:`ProcessorManager <darc.processor.ProcessorManager>` on the worker nodes and
:class:`ProcessorMasterManager <darc.processor_master.ProcessorMasterManager>` on the master node.
These create a new processor for each observation and keep track of which observations are running. When an
observation finishes processing, its processor exits and is evenutally removed from the observation list by a
thread scavenger. Each observation is uniquely identified by a task ID, as specified in the parset received from the
central Apertif control system.

Reading AMBER candidates
------------------------
The AMBER candidate reader described on the :ref:`real-time triggering page <_modules/triggers:Reading AMBER candidates>`
also sends its output to the real-time data processing system.

Clustering of AMBER candidates
------------------------------
The main processor instance takes care of parsing the AMBER candidates. The actual clustering is then run by
:class:`Clustering <darc.processor.processor_tools.clustering.Clustering>`. The clustering algorithm is identical
to that used by the :ref:`real-time triggering system <_modules/triggers:Clustering and thresholding>`. As in that system,
the user can set a S/N threshold, minimum DM, and maximum DM. There is no split between known and new sources.
The clustered triggers are written to an output file, and sent to the next step of the pipeline.

Data extraction
---------------
For each post-clustering trigger, the data have to be extracted from the filterbank file. This is done by
:class:`Extractor <darc.processor_tools.extractor.Extractor>`. The data extraction consists of several steps:

#. Calculate start and end time of filterbank chunk that is needed, based on arrival time, width, and DM
#. Wait until the required filterbank data are available on disk
#. Load the data
#. Optionally downsample if the DM smearing timescale is longer than the sampling time
#. Apply AMBER's RFI mask
#. Apply RFI cleaning
#. Dedisperse to DM as found by AMBER and determine optimized S/N and width
#. If S/N is lower than local S/N threshold: stop here, skip trigger
#. Dedisperse to a range of DMs around the AMBER value
#. Apply further downsampling in time (to match pulse width) and frequency (to reduce data size)
#. Roll the data to put the peak of the timeseries in the centre of the time window
#. Store the frequency-time (at AMBER DM), dm-time, and AMBER + optimized parameters in HDF5 format

Reading the filterbank data is done with :class:`ARTSFilterbankReader <darc.processor_tools.filterbank_reader.ARTSFilterbankReader>`.
This module reads in the tied-array beam data from disk and converts these to the requested SB.

For each candidate that passes the local S/N threshold, the path to the HDF5 file is sent to the next step of the pipeline.


Neural network classification of candidates
-------------------------------------------
Each candidate is processed by a convolutional neural network, developed by Liam Connor
(see also the corresponding `paper <https://ui.adsabs.harvard.edu/abs/2018AJ....156..256C/abstract>`_).
The :class:`Classifier <darc.processor_tools.classifier.Classifier>` module runs this classification. When an observation starts,
it initialises the frequency-time and dm-time models on the GPU. For each candidate, it then extracts the data from
the HDF5 file, reshapes them as needed, and classifies them. The output of the classifier is a probability of being a
real transient for both the frequency-time and the dm-time data. These probabilities are written to the HDF5 file.
If the probabilities are above the user-defined thresholds, the path to the HDF5 file is stored so it can be
picked up by the visualization stage.

Candidate visualization
-----------------------
After an observation finishes and all candidates have been processed, the
:ref:`classifier <_modules/processor:Neural network classification of candidates>` holds a list of HDF5 files,
each containing the data and metadata of a candidate that needs to be visualized. These are then visualized
by the :class:`Visualizer <darc.processor_tools.visualizer.Visualizer>` module. The user can set the number of
plots per page. For each candidate, a frequency-time, dm-time, and 1d-time plot is generated. The plots are sorted
by probability, then by S/N. Any candidate with a DM larger than the maximum Milky Way DM estimated from the YMW16
model gets a red border around the plot. The candidate metadata are shown in the plot title, using the optimized
parameters. Each page of plots is stored as a separate PDF file, and merged into one PDF after the plotting finishes.

Observation summary
-------------------
After the processing finishes, the PDF with candidates is copied to a central directory. Additionally,
a file with the metadata of all post-classifier triggers is created, as well as a summary file listing the number
of triggers at different steps in the pipeline.

The master node picks up these files and generates an email listing observation information and the trigger metadata,
with each PDF as attachment, as well as a simple web page with the same content. If one or more worker nodes that were
include in the observation do not create the required output files, the master node will eventually check if these
nodes are still online and processing the observation, and send a warning email if this is not the case.

.. note::
    If a worker node fails, an empty output file must be created for the master node to stop waiting for that node.
    The command to create that file is listed in the warning email that is sent when a node goes offline
