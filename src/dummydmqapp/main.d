/*******************************************************************************

    Infinitely running binary used as dummy tested application in turtle
    own tests to verify test helpers that work with DMQ.

    It keeps consuming on channel 'test_channel1' and writting new records to
    'test_channel2'.

    It should be never used for any other purpose.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dummydmqapp.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.io.Stdout;
import core.stdc.stdlib : abort;

import ocean.io.select.client.FiberSelectEvent;
import ocean.io.select.client.FiberTimerEvent;
import ocean.io.select.EpollSelectDispatcher;
import ocean.io.select.fiber.SelectFiber;

import swarm.client.helper.NodesConfigReader;
import swarm.client.plugins.ScopeRequests;
import swarm.util.Hash;
import dmqproto.client.DmqClient;

/*******************************************************************************

    Globals

*******************************************************************************/

alias ExtensibleDmqClient!(ScopeRequestsPlugin) DmqClient;

EpollSelectDispatcher epoll;
SelectFiber           fiber;
FiberSelectEvent      event;
DmqClient             dmq;

/*******************************************************************************

    Entry point. Creates all globals, performs handshake and starts
    infinite Consume request on "test_channel1".

*******************************************************************************/

void main ( )
{
    void initAndRegister ( )
    {
        dmq = new DmqClient(epoll, new ScopeRequestsPlugin, 2);
        dmq.addNodes("./etc/dmq.nodes");

        Stdout.formatln("Starting Consume request").flush();
        syncXtoY("test_channel1", "test_channel2");
    }

    epoll = new EpollSelectDispatcher;
    fiber = new SelectFiber(epoll, &initAndRegister, 256 * 1024);
    event = new FiberSelectEvent(fiber);

    fiber.start();
    epoll.eventLoop();
}

/*******************************************************************************

    Initiates Consume request on channel `src` that pushes all records
    to channel `dst`.

*******************************************************************************/

void syncXtoY ( cstring src, cstring dst )
{
    void record ( DmqClient.RequestContext c, in cstring value )
    {
        Stdout.formatln("Popped record '{}'", value).flush();
        (new Pusher(dst, value)).register();
    }

    void notify ( DmqClient.RequestNotification info )
    {
        if (info.type == info.type.Finished && !info.succeeded)
        {
            Stderr.formatln("ABORT: Consume failure").flush();
            abort();
        }
    }

    dmq.assign(dmq.consume(src, &record, &notify));
}

/*******************************************************************************

    Class that captures record to eventually push into
    target channel.

*******************************************************************************/

class Pusher
{
    cstring channel, value;

    this ( cstring channel, cstring value )
    {
        this.channel = channel.dup;
        this.value = value.dup;
    }

    void notify ( DmqClient.RequestNotification info )
    {
        if (info.type == info.type.Finished && !info.succeeded)
        {
            Stderr.formatln("ABORT: Put failure").flush();
            abort();
        }
    }

    cstring input ( DmqClient.RequestContext context )
    {
        return this.value;
    }

    void register ( )
    {
        dmq.assign(dmq.push(this.channel, &this.input, &this.notify));
    }
}
