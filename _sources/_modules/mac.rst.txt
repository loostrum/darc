Monitoring & control
====================

Control
-------
DARC is controlled by a master service, :class:`DARCMaster <darc.darc_master.DARCMaster>`.
This service controls all other services and takes care of starting/stopping observations.
It also holds the Python queues that connect different services together.
The :command:`darc_service` starts the master service. However, the user is advised to use one of the supplied
scripts to start DARC (see :ref:`scripts <_modules/mac:Scripts>`).

The master service listens for commands on a network port. DARC includes an executable to handle communication
with the master service (see :ref:`command line interface <_modules/mac:Command line interface>`),
but this can also be done from Python. For example::

    >>> from darc.control import send_command
    >>> output = send_command(timeout=5, service='processor', host='arts001', command='status')
    status: Success
    message: {'processor': 'running'}
    >>> output
    {'status': 'Success', 'message': {'processor': 'running'}}

``send_command`` returns a dictionary, unless something failed in which case it returns None.

Command line interface
^^^^^^^^^^^^^^^^^^^^^^
The user can interact with :class:`DARCMaster <darc.darc_master.DARCMaster>` through the ``darc`` executable.
All options can be listed by running ``darc -h``. A few examples::

    arts@arts041:~$ darc --service all status
    status: Success
    message: {'offline_processing': 'running', 'status_website': 'running', 'voevent_generator': 'running', 'lofar_trigger': 'running', 'processor': 'running'}

    arts@arts041:~$ darc --service lofar_trigger get_attr log_file
    status: Success
    message: {'lofar_trigger': "{'LOFARTrigger.log_file': /home/arts/darc/log/lofar_trigger.arts041.log}"}

    arts@arts041:~$ darc lofar_status
    status: Success
    message: LOFAR triggering is enabled

    arts@arts041:~$ darc --host arts001 --service amber_clustering restart
    status: Success
    message: {'amber_clustering': {'stop': 'stopped', 'start': 'started'}}

    arts@arts041:~$ darc --host nonexistent --service all status
    Failed to connect to DARC master: [Errno -2] Name or service not known

    arts@arts041:~$ darc stop_master
    status: Success
    message: Stopping master

.. note::
    The `start_observation` and `stop_observation` commands are normally executed by `ARTSSurveyControl` and should
    not be executed by the user.

Scripts
^^^^^^^
DARC comes with several scripts to make control of the pipeline easier:

* :command:`darc_start_master`: Starts the master service on the current node and checks whether it starts up properly.
  It also redirects the output to a log file located at ``$HOME/darc/log/darc_master.<hostname>.log``.
* :command:`darc_stop_master`: Stops the master service an by extension all other services, also aborting any
  running observations.
* :command:`darc_start_all_services`: Starts all services, including DARC Master if it is not running.
* :command:`darc_start_stop_all_services`: Stops all services except the master service. Aborts any running observation.
* :command:`darc_kill_all`: Kill master service and all other services. Use when DARC fails to exit using the normal
  stop command.

In addition, the following two commands are available on the ARTS cluster:

* :command:`start_full_pipeline`: Starts all DARC services on all nodes
* :command:`stop_full_pipeline`: Stops DARC services, including master service, on all nodes.

Monitoring
----------
Status website
^^^^^^^^^^^^^^
This is handled by the :class:`StatusWebsite <darc.status_website.StatusWebsite>` service, which runs on the master node.
It generates a simple web page showning whether or not the DARC services are online on each node
of the ARTS cluster. If a node cannot be reached, it turns grey. Otherwise each service on the node is checked and
shown in green if it is running, and in red if it is not.

Logging
^^^^^^^
Each service has its own log file, by default located at ``$HOME/darc/log/<service>.<hostname>.log``.
The log files include timestamps, allowing the user to check what happened at some point in the past.
