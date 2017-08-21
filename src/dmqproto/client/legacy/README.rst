.. contents ::

Introduction and Core Concepts
================================================================================

Please read the core client documentation before proceeding. This README
only describes features specific to the DMQ client.

The DMQ client enables asynchronous communication with a Distributed Message
Queue. A DMQ server can be spread over multiple 'nodes', enabling the bandwidth
and storage to be distributed.

The client can operate using multiple connections to each DMQ node. This,
combined with the distribution of the DMQ across multiple nodes, leads to the
possibility of many requests being exectued in parallel (asynchronously -- the
asynchronous performance of requests is handled by epoll).

Basic DMQ Client Usage
================================================================================

Empty Records
--------------------------------------------------------------------------------

It is not possible to store empty records in the DMQ. The client checks this and
will cancel the request if the user attempts to write an empty record.

Pop Delegates
--------------------------------------------------------------------------------

Requests which read data from the DMQ must provide a delegate which is to be
called when the data is received. Requests which read multiple pieces of data
from the DMQ (Consume, for example) will result in the provided delegate being
called multiple times, once for each piece of data received.

Push Delegates
--------------------------------------------------------------------------------

Requests which write data to the DMQ must provide a delegate which is to be
called when the client is ready to send the data for the request, and must
return the data to be sent. Requests which write multiple pieces of data to the
DMQ (Produce, for example) will result in the provided delegate being called
multiple times, sending each provided piece of data until the delegate returns
an empty string to indicate the end of the list.

Note that the data provided by the push delegate is only sliced by the DMQ
client, and must remain available until the request finished notification is
received.

Produce Delegates
--------------------------------------------------------------------------------

Unlike the standard Push/Multi requests, when a Produce/Multi request is being
handled by the client, the records which are passed to the ``IProducer``
interface *are* copied internally, rather than sliced. This is because the
application receives no specific feedback indicating when a record has been
sent, thus would not be able to know when to recycle the buffer that was holding
the record.

Consume and Produce Requests
--------------------------------------------------------------------------------

An alternative to the simple Push and Pop commands, the DMQ supports streaming
pushing and popping via the Produce and Consume requests. The difference is that
the node does not send a status response for each single transaction, instead
the client continuously sends a stream of records to the node (Produce) or
receives it from the node (Consume). This has two major advantages:

 * The client needs only one connection per node to accomplish the maximum
   possible throughput because the messages are sent in a pipeline fashion.

 * The application is notified via a callback when a record can be sent to
   (Produce) or was received from (Consume) the node. This mechanism allows the
   application to work in an event based way, excludes the possibility of
   request queue overflows and requires a much simpler notification handling
   logic than the Push and Pop request.

The disadvantage of the streaming requests is that, should the network
connection between server and client break, all messages in flight are lost
because the message sender -- the client for Produce or the node for Consume --
has no way of telling whether a particular message has arrived or not.

Basic Usage Example
--------------------------------------------------------------------------------

See dmqproto.client.DmqClient module header.

Multi-Node DMQs
================================================================================

The DMQ client is able to connect with and send requests to a DMQ system
consisting of one or more nodes. In the case where more than a single node
exists, Push/PushMulti and Pop requests are routed to the individual nodes using
a round-robin policy.

Produce Requests
--------------------------------------------------------------------------------

Multiple ``IProducer`` objects will be handed to the application via the produce
delegate, and the application should use them in the order it receives them to
balance the load across the DMQ nodes. The maximum number of ``IProducer``s the
application has to keep is the number of DMQ nodes.

Example:

.. code:: D

    import dmqproto.client.DmqClient;
    import ocean.util.container.queue.FixedRingQueue;

    auto dmq = new DmqClient(/* ... */);
    // Add nodes to dmq...

    auto producers = new FixedRingQueue!(DmqClient.IProducer)(dmq.nodes.length);

    void receiveProducer ( DmqClient.IProducer producer )
    {
        // It is safe to assume that this function gets called only if there is
        // space in the producers queue.
        producers.push(producer);
    }

    void notify ( DmqClient.RequestNotification info ) { /* ... */ }

    dmq.assign(dmq.produce("my_channel", &receiveProducer, &notify));

While the event loop is running this program should send records to the DMQ
nodes by doing this:

.. code:: D

    DmqClient.IProducer producer;
    if (producers.pop(producer))
    {
        producer("message to be pushed in the DMQ");
    }
    else
    {
        // Not ready to produce -- wait until receiveProducer() is called again.
    }

Consume Requests
--------------------------------------------------------------------------------

Consume requests work in the same way for multiple as for single DMQ nodes.

User notification and error handling for Push/PushMulti and Pop requests
--------------------------------------------------------------------------------

Due to the current Swarm client architecture the type of notification for a
request that finished -- either successfully or with an error -- differs between
Push/PushMulti and Pop. The handling of errors in a multi-node DMQ is a special
case: when a request is assigned and fails for a node for some reason, the
client will automatically re-assign the request to the next node in the system.
This will continue until either the request succeeds or all nodes have been
tried and have failed.

Suppose you are using this request notification callback:

.. code:: D

    void notify ( DmqClient.RequestNotification info )
    {
        // How do I know if a request succeeded or failed?
    }


* For each Push/PushMulti/PushMulti2 request that has finished ``notify()`` is
  called once with ``info.type == info.type.GroupFinished``, and
  ``info.exception`` is `null` if the request succeeded or reflects the error if
  it failed. If it failed then it has been tried with all nodes, and ``info``
  contains the last error.

* For each Pop request that has finished ``notify()`` is called once with
  ``info.type == info.type.GroupFinished``. If a Pop request failed for a node
  or the DMQ channel in the node was empty then it is reassigned to another node
  until all nodes have been tried. Currently there is no way of telling from
  `info` whether a request that has finished has altogether succeeded or failed.
