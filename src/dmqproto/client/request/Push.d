/*******************************************************************************

    Client DMQ Push request definitions / handler.

    The Push request attempts to push one record to the specified DMQ
    channel(s). This works as follows:
        1. The client selects a connected DMQ node from its registry at random.
        2. A request is sent to the selected node, asking for the specified
           record to be added to the specified channel(s).
        3. If that node cannot handle the request (due to an error), then
           another node is selected and steps 2 and 3 are repeated.
        4. The request ends when either the record is pushed to a node or all
           available DMQ nodes have been queried and could not handle the
           request.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.Push;

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
    mstring[] channels;
    void[] value;
}

/*******************************************************************************

    Union of possible notifications.

*******************************************************************************/

private union NotificationUnion
{
    /// The request succeeded.
    RequestInfo success;

    /// The request was tried on a node and failed due to a connection error;
    /// it will be retried on any remaining nodes.
    NodeExceptionInfo node_disconnected;

    /// The request was tried on a node and failed due to an internal node
    /// error; it will be retried on any remaining nodes.
    NodeInfo node_error;

    /// The request was tried on a node and failed because it is unsupported;
    /// it will be retried on any remaining nodes.
    RequestNodeUnsupportedInfo unsupported;

    /// The request tried all nodes and failed.
    RequestInfo failure;
}

/*******************************************************************************

    Notification smart union.

*******************************************************************************/

public alias SmartUnion!(NotificationUnion) Notification;

/*******************************************************************************

    Type of notifcation delegate.

*******************************************************************************/

public alias void delegate ( Notification, const(Args) ) Notifier;
