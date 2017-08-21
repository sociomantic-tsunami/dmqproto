/*******************************************************************************

    Protocol definition of the DMQ Pop request.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.common.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.neo.request.Command;

/*******************************************************************************

    Status code enum. Sent from the node to the client.

*******************************************************************************/

public enum RequestStatusCode : StatusCode
{
    None,   // Invalid, default value

    Popped, // Value popped
    Empty,  // Value not popped because the specified channel is empty
    Error,  // Internal node error occurred
    Subscribed // Cannot pop, the channel has a subscriber
}
