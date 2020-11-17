Real-time triggering
====================

Introduction
------------
The ARTS GPU cluster receives both a Stokes I and Stokes IQUV data stream from the
beamformer, and searches the Stokes I data for transients in real-time using a GPU pipeline,
`AMBER <https://www.github.com/TRASAL/AMBER>`_. The real-time triggering part of DARC takes
the candidate metadata produced by AMBER, and decides whether or not to send a trigger to store
IQUV data, observe with the LOFAR transient buffer boards (TBBs), or send a VOEvent to the outside world.

Each of the 40 worker nodes of the GPU cluster processes the data from one Compound Beam (CB). Each CB
points in a different direction of the sky, so the nodes can operate mostly independently from one another.
The notable exception to this is external triggers: LOFAR and VOEvents. While the worker nodes identify potential
external triggers, actually sending the triggers is taken care of by the master node. On this page, each step of the
real-time triggering system is explained in more detail.

Reading AMBER candidates
------------------------
Each worker node runs three instances of AMBER, each searching a different part of parameter space.
The candidate metadata is written to a text file, one file per AMBER instance. These files are read by
the :class:`AMBERListener <darc.amber_listener.AMBERListener>` module, in a
way similar to the :command:`tail -f` command in bash. Each line of an AMBER file, containing the metadata of one candidate,
is fed to the next modules as-is, i.e. as one string. As the triggers with different DMs and the same arrival time
are detected by AMBER at different times, the candidates as output by
:class:`AMBERListener <darc.amber_listener.AMBERListener>` are not necessarily in order of arrival time.

Clustering and thresholding
---------------------------
:class:`AMBERClustering <darc.amber_clustering.AMBERClustering>` takes the AMBER candidates and decides whether to
send triggers to different parts of the system. It first parses the triggers based on the header (which is also
sent by the :ref:`AMBER listener module <_modules/triggers:Reading AMBER candidates>`). At a configurable interval,
all triggers that arrived during that interval are clustered together. The clustering algorithm was originally developed
by Liam Connor as part of `arts-analysis <https://github.com/TRASAL/arts-analysis>`_. It clusters candidates in time
and DM, automatically selecting the optimal width and Synthesized Beam (SB) index. In addition, it has an SB periodicity
filter developed by Dany Vohl, which can be enabled in the DARC config file.
If a known source is being observed, the clustering and following triggering runs twice: once for triggers with a DM
close to that of the known source, and once for any DM.
An S/N limit, minimum DM, and maximum DM can be set in the config file.

After clustering, each remaining
trigger is checked against the IQUV trigger thresholds. These include a limit on width (for new sources) and DM (at least
some fraction of the maximum Milky Way DM estimated from the YMW16 model). There is also a limit on how often IQUV triggers
can be executed, because the disks cannot handle writing IQUV data continuously. If a trigger passes all these checks,
it is sent to the :ref:`IQUV triggering system <_modules/triggers:IQUV triggering>`.

The triggers passing the IQUV thresholds are then checked against the LOFAR triggering thresholds. As LOFAR triggering can
only be done for the best candidates, the LOFAR trigger thresholds are always assumed to be more strict than the IQUV
thresholds. LOFAR triggering can set to run for all sources, only for known sources, or only for a specific list of
sources listed in the config file. Each type has separate user-configurable thresholds in DM, S/N, and width.
For the specific sources, the system can also be set to only trigger on the central few CBs, to reduce the false
positive rate. Triggers passing the LOFAR thresholds are sent to both the
:ref:`LOFAR trigger system <_modules/triggers:LOFAR triggering>` and
the :ref:`VOEvent system <_modules/triggers:Sending VOEvents>`.

IQUV triggering
---------------
The IQUV data are buffered using a `PSRDADA <https://psrdada.sourceforge.net/>`_ ringbuffer. PSRDADA's `dada_dbevent`
listens on a network port for a trigger. A trigger contains the UTC start/end time of the data that should be stored,
and several burst parameters. If the ringbuffer still contains the requested data, these are copied to a second ringbuffer
to which `dada_dbdisk` is connected, which then writes the data to disk.

.. note::
    The data as written by `dada_dbdisk` are not in a nicely
    readable format. A tool is available to convert the data
    to PSRFITS: `dadafits <https://github.com/TRASAL/dadafits>`_.
    `dadafits` reads data from a ringbuffer, so the
    data on disk must be put into a ringbuffer with
    `dada_diskdb`. `dadafits` cannot run in real-time
    because the data conversion is a slow process.

:class:`DADATrigger <darc.dada_trigger.DADATrigger>` takes care of sending IQUV triggers to `dada_dbevent`.
Using the burst arrival time and dispersion measure, it determines which chunk of data should be stored to disk.

:class:`DADATrigger <darc.dada_trigger.DADATrigger>` is also capable of sending Stokes I triggers.
This works exactly the same as IQUV triggers, but the trigger is sent to a second `dada_dbevent` listening on
a different network port. This mode is not currently used. Instead, the Stokes I data are always read from
the filterbank data on disk (See also the :ref:`data extraction <_modules/processor:Data extraction>` section
of the real-time data processing section).

.. warning::
    Sending a Stokes I trigger but not reading the triggered data from the ringbuffer can cause the entire observation to
    stall.

Polarization calibration
^^^^^^^^^^^^^^^^^^^^^^^^
In order to calibrate IQUV data, typically one or more calibrator sources are observed. Some IQUV data should be stored
during these observations, even if there are no AMBER triggers.
In order to facilitate this, :class:`DADATrigger <darc.dada_trigger.DADATrigger>` has a polarization calibration mode.
This mode is activated when

* The source name contains "polcal"
* The source was explicitly put in the CB processed by this worker node

In polarization calibration mode, the :class:`DADATrigger <darc.dada_trigger.DADATrigger>` sends IQUV triggers
at regular intervals. The length of each trigger and interval can be configured.

.. note::
    In polarization calibration mode, regular triggering is disabled.

    IQUV triggers should be no longer than 30 s in duration, and have at least an interval equal to the trigger duration.
    As the data rate is higher than the writing speed of the disks, this ensures the disks can keep up.

LOFAR triggering
----------------
The master node takes care of LOFAR triggers using :class:`LOFARTrigger <darc.lofar_trigger.LOFARTrigger>`. This module
runs a server, that all other nodes can send their potential LOFAR triggers to.
When :class:`LOFARTrigger <darc.lofar_trigger.LOFARTrigger>` receives a trigger through this server, it waits
for a short time to check if other nodes have sent triggers as well. A bright transient can show up
in several CBs, so this is quite likely. If several triggers are received, preference is given to triggers from known
sources. Then the trigger with the highest S/N is chosen.

From the trigger arrival time at ARTS and its DM, :class:`LOFARTrigger <darc.lofar_trigger.LOFARTrigger>` calculates
the arrival time at LOFAR frequencies. It then aims to have the signal in the centre of the few-second TBB buffer when
the buffers are frozen. To execute the actual buffer freeze, a message is sent to a server at LOFAR containg the
buffer freeze time accurate to a millisecond, the DM accurate to 0.1 pc cm :sup:`-3`, and a flag indicating
whether the message is a test or a real observation.

.. note::
    To check whether the LOFAR trigger module is enabled/disabled, or check its status, DARC supports the
    `lofar_enable`, `lofar_disable`, and `lofar_status` commands. These commands must be sent to the master node.

Sending VOEvents
----------------
The VOEvent system is very similar to the :ref:`LOFAR trigger system <_modules/triggers:LOFAR triggering>`.
If it receives multiple triggers, it selects the trigger with the highest S/N. Unlike the LOFAR trigger system, it does
not give preference to known sources. For the selected trigger, an XML-format VOEvent is generated following the
`community standard <https://github.com/ebpetroff/FRB_VOEvent>`_. This event is then sent to the VOEvent broker at
ASTRON using `comet <https://comet.transientskp.org/en/stable/>`_.

.. note::
    To check whether the VOEvent module is enabled/disabled, or check its status, DARC supports the
    `voevent_enable`, `voevent_disable`, and `voevent_status` commands. These commands must be sent to the master node.
