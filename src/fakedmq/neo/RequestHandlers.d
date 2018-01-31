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

import Consume = fakedmq.neo.request.Consume;
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
    request_handlers.add(RequestCode.Consume, "consume", &Consume.handle);
    request_handlers.add(Command(RequestCode.Push, 2), "push", PushImpl_v2.classinfo);
    request_handlers.add(Command(RequestCode.Pop, 0), "pop", PopImpl_v0.classinfo);
}
