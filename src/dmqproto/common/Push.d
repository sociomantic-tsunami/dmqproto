/*******************************************************************************

    Protocol definition of the DMQ Push request.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.common.Push;

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

    Pushed, // Value pushed
    Error   // Internal node error occurred
}
