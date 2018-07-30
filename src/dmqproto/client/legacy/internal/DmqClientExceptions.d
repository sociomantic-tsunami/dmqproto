/*******************************************************************************

    Custom exception types which can occur inside a DMQ client. Instances of
    these exception types are passed to the user's notification delegate to
    indicate which error has occurred in the client. They are not necessarily
    actually thrown anywhere (though some are).

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.DmqClientExceptions;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.ClientExceptions;



/*******************************************************************************

    Exception passed to user notifier when a request which uses the
    reregistrator (see dmqproto.client.legacy.internal.connection.model.IReregistrator) has
    tried all nodes and failed.

*******************************************************************************/

public class AllNodesFailedException : ClientException
{
    public this ( )
    {
        super("Request failed on all DMQ nodes");
    }
}

