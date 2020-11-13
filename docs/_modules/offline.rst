Offline data processing
=======================

Introduction
------------
The offline processing pipeline takes care of analyses that can only run after the data recording of an observation
finishes. This is handled by the :class:`OfflineProcessing <darc.offline_processing.OfflineProcessing>` module.

.. note::
  The offline processing module is also capable of running a complete candidate analysis, but that part has been
  superseded by the :ref:`real-time data processing <_modules/processor:Real-time data processing>`.
  The complete candidate analysis is still available in the current offline processing module, but disabled and
  undocumented.

Pulsar folding
--------------
If a known test pulsar (as defined in the source list file) is being observed, and it is explicitly located in
the compound beam that is processed by the current worker node, the filterbank data of tied-array beam zero
are folded at the pulsar ephemeris with PRESTO's ``prepfold``. The resulting postscript plot is converted to PDF.
An external tool picks up these figures, as well as any output figures from the real-tie pipeline,
and copies them to the Apertif surveys wordpress website, so they can be easily inspected without logging in to the system.


Drift scan calibration
----------------------
An external tool to measure the sensitivity of the system based on drift scans of known calibrator sources is available,
and called automatically when :class:`OfflineProcessing <darc.offline_processing.OfflineProcessing>` detects that

* The source name contains a known calibrator (as determined from the config file)
* The current compound beam is part of the drift scan

This requires source names of the format ``<source>drift<first_beam><last_beam>``, or ``<source>drift<beam>`` in case of a drift
through one compound beam. ``<beam>`` should always be a two-digit number. For example: ``3C286drift0107`` would be
a drift scan through the top row of compound beams.

The output of the drift scan calibration consists of a numpy file for each TAB of each CB, containing downsampled
filterbank data, and a figure showing a spectrum and different sensitivity metrics. These plots are stored in a folder
named ``<date>_<source>`` within the calibration directory specified in the config file.

Deep search for known FRB sources
---------------------------------
An external tool that visualizes any of the post-clustering candidates if their DM is close to that of a known source
and that known source is in the field, is available and automatically called after every observation.
