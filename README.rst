CORD Workflow Controller Client
===============================
A CORD Workflow Controller Client Library.
This library allows users to communicate to CORD Workflow Controller.
There are three classes provided, Manager, Probe and WorkflwoRun.

Manager
-------
This class is used to act as a workflow manager.
Workflow Managers manage **workflow registration** and handle **kickstart** events.

Probe
-----
This class is used to act as a probe.
Probes emit events to CORD Workflow Controller.

WorkflowRun
-----------
This class is used to act as a workflow run.
Workflow Runs are used by workflow instances to receive events emitted by Probes.
