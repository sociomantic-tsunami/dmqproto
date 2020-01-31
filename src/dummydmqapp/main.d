/*******************************************************************************

    Infinitely running binary used as dummy tested application in turtle
    own tests to verify test helpers that work with DMQ.

    It keeps consuming on channel 'test_channel1' and writting new records to
    'test_channel2'.

    It should be never used for any other purpose.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dummydmqapp.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
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

EpollSelectDispatcher epoll;
SelectFiber           fiber;
FiberSelectEvent      event;
DmqClient             dmq;

/*******************************************************************************

    Entry point. Creates all globals, performs handshake and starts
    infinite Consume request on "test_channel1".

*******************************************************************************/

version ( unittest ) {} else
void main ( )
{
    void initAndRegister ( )
    {
        dmq = new DmqClient(epoll, 2);
        dmq.addNodes("./etc/dmq.nodes");

        Stdout.formatln("Starting Consume request").flush();
        (new Sync("test_channel1", "test_channel2")).register();
    }

    epoll = new EpollSelectDispatcher;
    fiber = new SelectFiber(epoll, &initAndRegister, 256 * 1024);
    event = new FiberSelectEvent(fiber);

    fiber.start();
    epoll.eventLoop();
}


/*******************************************************************************

    Initiates Consume request on channel `src` that writes all records
    to channel `dst`.

*******************************************************************************/

class Sync
{
    import core.thread;
    import ocean.core.Time;

    cstring src;
    cstring dst;

    this ( cstring src, cstring dst )
    {
        this.src = src;
        this.dst = dst;
    }

    void record ( DmqClient.RequestContext c, in cstring value )
    {
        Stdout.formatln("Syncing '{}'", value).flush();
        (new Pusher(this.dst, value)).register();
    }

    void notify ( DmqClient.RequestNotification info )
    {
        if (info.type == info.type.Finished && !info.succeeded)
        {
            // some tests use node restart functionality - to make this simple
            // app compatible with them, listen request needs to be restarted
            // upon failures until the app gets killed
            Stderr.formatln("Listen failure, trying again in 100 ms").flush();
            Thread.sleep(seconds(0.1));
            (new Sync(this.src, this.dst)).register();
        }
    }

    void register ( )
    {
        Stdout.formatln("Starting sync from {} to {}", this.src, this.dst).flush();
        dmq.assign(dmq.consume(this.src, &this.record, &this.notify));
    }
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
