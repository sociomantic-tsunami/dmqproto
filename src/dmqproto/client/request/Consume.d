/*******************************************************************************

    Client DMQ Consume request public definitions.

    The Consume request receives all records that are pushed to a DMQ channel.
    If you assign a Consume request it will keep going until you request to
    stop it or the channel is deleted.

    The Consume request has two parameters:
      1. Channel name,
      2. Subscriber name (optional, an empty string by default).

    The meaning of the channel name is obvious: It specifies from which channel
    the pushed records should be received. The subscriber name is a string
    identifier you can freely choose (only ASCII alphanumeric characters and
    '_' are allowed, though). It refers to the principle that by assigning a
    Consume request you *subscribe* to a channel; you are a *channel subscriber*
    then.
    The system of channel subscribers is relevant if a channel has multiple
    subscribers; that is, multiple clients have assigned Consume requests to the
    same channel. It works as follows:
     - When assigning a Consume request for a channel each client uses a
       subscriber name. By default, if no subscriber name argument is passed to
       `DmqClient.Neo.consume()`, the subscriber name is an empty string.
     - The DMQ node maintains one internal queue per channel and subscriber
       name.
       - Each individual subscriber receives the records that are pushed to the
         queue matching its subscriber name.
       - If multiple clients have assigned a Consume requests to the same
         channel using the same subscriber name then these clients share the
         same queue.
     - If a record is now pushed to the channel the node pushes it to all
       subscriber queues of the channel.
     - If a DMQ channel has no subscribers but contains records and you assign
       a Consume request for that channel (i.e. you are the first subscriber)
       then you receive all records that are currently in the channel.
       Subsequent subscribers (using a different subscriber name than you use)
       will only receive the records that are pushed after they assigned their
       Consume request.
     - Subscriptions are permanent. If you stop your Consume request and no
       other Consume request is assigned with the subscriber name you used then
       the subscriber queue will stay, and the next time a Consume request is
       assigned to the channel with the subscriber name you used the client
       that submitted that request will receive all records that have been
       pushed to the channel in the mean time.

    Example of a scenario with only one subscriber name:
      - Process A pushes n records to DMQ channel "ch" which didn't exist
        before so the channel is created.
      - Process B assigns a Consume request for "ch" passing no subscriber name
        to `DmqClient.Neo.consume()` thus using the default "" subscriber name.
      - Process C does the same as process B.
      - Each of the records A had pushed before B and C had started the Consume
        request goes to either B or C.
      - A pushes a record: The record goes to either B or C.
      - B disconnects or goes down: C receives all records pushed by A.
      - C disconnects or goes down, too: The DMQ node stores the records pushed
        by A.
      - Some process D (can be B or C again, or yet another process) starts a
        Consume request for "ch" using the default empty subscriber name: That
        process receives the records pushed by A in the mean time.

    If B, C and D specify a subscriber name, say "sub1", to
    `DmqClient.Neo.consume()` then things keep working in the same way.

    Example of a scenario with two subscriber names:
      - Process A pushes n records to DMQ channel "ch" which didn't exist
        before so the channel is created.
      - Process B assigns a Consume request for "ch" using subscriber name
        "sub1" and receives all records previously pushed by A.
      - Process C assigns a Consume request for "ch" using subscriber name
        "sub2" and receives no record for now.
      - A pushes a record to "ch": The DMQ node sends one copy of the record to
        B and another copy to C.
      - Process D assigns a Consume request to "ch" using subscriber name "sub2"
        (same as D) and receives no record for now.
      - A pushes a record to "ch": The DMQ node sends one copy of the record to
        B and another copy to either C or D.
      - B goes down while A keeps pushing records: The DMQ node stores a copy of
        each record and sends another copy to either C or D.
      - Some process E (can be B again or yet another process) starts a Consume
        request for "ch" using subscriber name "sub1": E receives the records
        pushed by A since B went down.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import ocean.core.SmartUnion;
public import swarm.neo.client.NotifierTypes;
import dmqproto.common.Consume;

/*******************************************************************************

    Request-specific arguments provided by the user and passed to the notifier.

*******************************************************************************/

public struct Args
{
    mstring channel;
    mstring subscriber;
}

/*******************************************************************************

    Enum which is passed to notifications. As the request is handled by all
    known nodes simultaneously, some notifications occur on a per-node basis.

*******************************************************************************/

private union NotificationUnion
{
    /// A value is received from a node.
    RequestDataInfo received;

    /// The connection to a node disconnected; the request will automatically
    /// continue after reconnection.
    NodeExceptionInfo node_disconnected;

    /// All known nodes have either stopped the request (as requested by the
    /// user, via the controller) or are not currently connected. The request is
    /// now finished.
    RequestInfo stopped;

    /// The node returned a non-OK status code. The request cannot be handled by
    /// this node.
    NodeInfo node_error;

    /// The request was tried on a node and failed because it is unsupported;
    /// it will be retried on any remaining nodes.
    RequestNodeUnsupportedInfo unsupported;

    /// The channel being consumed has been removed. The request is now
    /// finished for this node.
    NodeInfo channel_removed;
}

/*******************************************************************************

    Notification smart union.

*******************************************************************************/

public alias SmartUnion!(NotificationUnion) Notification;

/*******************************************************************************

    Type of notification delegate.

*******************************************************************************/

public alias void delegate ( Notification, const(Args) ) Notifier;

/*******************************************************************************

    Request controller, accessible via the client's `control()` method.

*******************************************************************************/

public interface IController
{
    /***************************************************************************

        Suspends this request. While the request is suspended the node will not
        send records, and records that have already been received will not be
        passed to the user notifier but held back until `resume` is called.

        Returns:
            true because this controller function can always be used.

    ***************************************************************************/

    bool suspend ( );

    /***************************************************************************

        Resumes this request.

        Returns:
            true because this controller function can always be used.

    ***************************************************************************/

    bool resume ( );

    /***************************************************************************

        Tells the nodes to cleanly end the request.
        Records that have already been received from the node will still be
        passed to the user. In this situation it is still possible to use
        `suspend` and `resume`.

        Returns:
            true because this controller function can always be used (it has an
            effect only the first time it is called, though).

    ***************************************************************************/

    bool stop ( );

    /***************************************************************************

        Returns:
            `true` if the nde is suspended, otherwise, `false`.

    ***************************************************************************/

    bool suspended ( );
}
