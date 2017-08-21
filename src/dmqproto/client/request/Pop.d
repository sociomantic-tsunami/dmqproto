/*******************************************************************************

    Client DMQ Pop request definitions / handler.

    The Pop request attempts to pop one record from the specified DMQ channel.
    This works as follows:
        1. The client selects a connected DMQ node from its registry at random.
        2. A request is sent to the selected node, asking for a record from the
           specified channel.
        3. If the channel on that node contains data, one record is popped and
           returned to the client. If no records are available in the channel on
           that node, then another node is selected and steps 2 and 3 are
           repeated.
        4. The request ends when either a record is sent to the client or all
           available DMQ nodes have been queried and returned no data.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.core.SmartUnion;
public import swarm.neo.client.NotifierTypes;

/*******************************************************************************

    Request-specific arguments provided by the user and passed to the notifier.

*******************************************************************************/

public struct Args
{
    mstring channel;
}

/*******************************************************************************

    Union of possible notifications.

*******************************************************************************/

private union NotificationUnion
{
    /// The request succeeded, but the channel was empty.
    RequestInfo empty;

    /// The request succeeded and a value was popped.
    RequestDataInfo received;

    /// No nodes were connected, so the request could not be tried on any node.
    RequestInfo not_connected;

    /// The request tried all nodes, did not receive a value, and experienced
    /// at least one error.
    RequestInfo failure;

    /// The request was tried on a node and failed due to a connection error;
    /// it will be retried on any remaining nodes.
    NodeExceptionInfo node_disconnected;

    /// The request was tried on a node and failed because the channel has
    /// subscribers; it will be retried on any remaining nodes.
    NodeInfo channel_has_subscribers;

    /// The request was tried on a node and failed due to an internal node
    /// error; it will be retried on any remaining nodes.
    NodeInfo node_error;

    /// The request was tried on a node and failed because it is unsupported;
    /// it will be retried on any remaining nodes.
    RequestNodeUnsupportedInfo unsupported;
}

/*******************************************************************************

    Notification smart union.

*******************************************************************************/

public alias SmartUnion!(NotificationUnion) Notification;

/*******************************************************************************

    Type of notifcation delegate.

*******************************************************************************/

public alias void delegate ( Notification, Args ) Notifier;
