/*******************************************************************************

    Table of request handlers by command.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.RequestHandlers;

import swarm.neo.node.ConnectionHandler;

import dmqproto.common.RequestCodes;

import fakedmq.neo.request.Consume;
import fakedmq.neo.request.Push;
import fakedmq.neo.request.Pop;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.RequestMap request_handlers;

static this ( )
{
    request_handlers.addHandler!(ConsumeImpl_v4);
    request_handlers.addHandler!(PushImpl_v3);
    request_handlers.addHandler!(PopImpl_v1);
}
