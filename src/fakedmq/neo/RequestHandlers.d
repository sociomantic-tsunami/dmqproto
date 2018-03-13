/*******************************************************************************

    Table of request handlers by command.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.RequestHandlers;

import swarm.neo.node.ConnectionHandler;
import swarm.neo.request.Command;

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
    request_handlers.add(Command(RequestCode.Consume, 4), "consume", ConsumeImpl_v4.classinfo);
    request_handlers.add(Command(RequestCode.Push, 3), "push", PushImpl_v3.classinfo);
    request_handlers.add(Command(RequestCode.Pop, 1), "pop", PopImpl_v1.classinfo);
}
