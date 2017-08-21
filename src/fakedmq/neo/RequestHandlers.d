/*******************************************************************************

    Table of request handlers by command.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.RequestHandlers;

import swarm.neo.node.ConnectionHandler;

import dmqproto.common.RequestCodes;

import Consume = fakedmq.neo.request.Consume;
import Push    = fakedmq.neo.request.Push;
import Pop     = fakedmq.neo.request.Pop;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.CmdHandlers request_handlers;

static this ( )
{
    request_handlers[RequestCode.Consume] = &Consume.handle;
    request_handlers[RequestCode.Push]    = &Push.handle;
    request_handlers[RequestCode.Pop]     = &Pop.handle;
}
