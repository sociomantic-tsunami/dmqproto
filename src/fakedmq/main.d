/*******************************************************************************

    Example fake queue node. Can be used to debug the
    protocol changes manually (in a more controlled environment).

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import fakedmq.DmqNode;

import dmqproto.client.legacy.DmqConst;

import ocean.io.select.EpollSelectDispatcher;

import ocean.util.log.Log;
import ocean.util.log.AppendStderrStdout;
import ocean.util.log.AppendConsole;

/*******************************************************************************

    Configure logging

*******************************************************************************/

static this ( )
{
    Log.root.add(new AppendStderrStdout);
    Log.root.level(Level.Info, true);
}

/*******************************************************************************

    Simple app that starts fake queue and keeps it running indefinitely until
    the processed is killed.

*******************************************************************************/

void main ( )
{
    auto epoll = new EpollSelectDispatcher;
    auto node = new DmqNode(DmqConst.NodeItem("127.0.0.1".dup, 10000), epoll);

    Log("Registering the fake node");
    node.register(epoll);

    Log("Starting infinite event loop, kill the process if not needed anymore");
    epoll.eventLoop();
}
