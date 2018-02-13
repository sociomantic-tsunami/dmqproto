/*******************************************************************************

    Protocol definition of the DMQ Consume request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.common.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.neo.request.Command;

/*******************************************************************************

    Status code enum. Sent from the node to the client.

*******************************************************************************/

public enum RequestStatusCode : StatusCode
{
    None,    // Invalid, default value

    Started, // Consume started
    Error    // Internal node error occurred
}

/*******************************************************************************

    Message type enum for Consume v1 and v2. Each message sent between the
    client and the node as part of a Consume request is prepended by a type indicator.

*******************************************************************************/

public enum MessageType : ubyte
{
    None,       // Invalid, default value

    // Message types sent from the client to the node:
    Suspend,    // Sent when the client wants the node to stop sending records
    Resume,     // Sent when the client wants the node to restart sending records
    Stop,       // Sent when the client wants the node to cleanly end the request
    ChannelRemoved, // Sent when the channel being consumed is removed

    // Message types sent from the node to the client:
    Ack,        // Sent by the node to acknowledge one of the above messages
    Record      // Sent by the node when it sends a record value
}

/// Message type enum for Consume v3.
public enum MessageType_v3 : ubyte
{
    None,            // Invalid, default value

    // Message types sent from the client to the node:
    Continue,   // Requesting the next batch of records
    Stop,       // Requesting to cleanly end the request

    // Message types sent from the node to the client:
    Records,    // The message contains a batch of records
    Stopped,    // Acknowledging the request has ended
    ChannelRemoved, // The channel being consumed is removed
}

/*******************************************************************************

    Consume request start state enum. Enables the client to start a Consume
    request in the suspended state.

*******************************************************************************/

public enum StartState : ubyte
{
    Running,
    Suspended
}
