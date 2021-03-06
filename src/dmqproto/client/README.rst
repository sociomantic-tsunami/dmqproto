DMQ Client Overview
===================

This package contains the client to connect to a Distributed Message Queue
(DMQ). The client is built on top of the framework provided by
`swarm <https://github.com/sociomantic-tsunami/swarm/>`_. Swarm (and thus the DMQ client)
currently supports two protocols, known as the "legacy protocol" and the "neo
procotol". Detailed documentation about the workings of swarm-based clients and
the two protocols can be found
`here (legacy protocol) <https://github.com/sociomantic-tsunami/swarm/blob/v5.x.x/src/swarm/README_client.rst>`_
and `here (neo protocol) <https://github.com/sociomantic-tsunami/swarm/blob/v5.x.x/src/swarm/README_client_neo.rst>`_.

The legacy protocol is to be phased out, so the remainder of this README focuses
solely on the neo protocol.

Requests
--------

Request API Modules
...................

Each request has an API module containing:

* A description of what the request does and how it works.
* The definition of the notifier delegate type for the request. (A notifier
  must be provided by the user, when assigning a request, and is called whenever
  anything of interest related to the request happens.)
* The ``Args`` struct which is passed to the notifier delegate. This contains a
  copy of all arguments which were specified by the user to start the request.
* The smart union of notifications which is passed to the notifier. The active
  member of the union indicates the type of the notification and may carry
  additional information (e.g. the address/port of a node, an exception, etc).

The request API modules provide a single, centralised point of documentation and
definitions pertaining to each request.

Available Requests
..................

The DMQ supports the following requests (links to the API modules):

* `Push <request/Push.d>`_:
  pushes a record to one or more channels.
* `Pop <request/Pop.d>`_:
  pops a record from a channel.
* `Consume <request/Consume.d>`_:
  receives a stream of records which have been pushed to a channel.

Assigning Requests
..................

The methods to assign requests are in the ``DmqClient`` class and defined in
`this module <mixins/NeoSupport.d>`_, along with detailed usage examples. Note
that there are two ways to assign some requests:

1. Via the ``DmqClient.neo`` object. This assigns a request in the normal
   manner.
2. Via the ``DmqClient.blocking`` object. This assigns a request in a ``Task``-
   blocking manner -- the current task will be suspended until the assigned
   request is finished.
