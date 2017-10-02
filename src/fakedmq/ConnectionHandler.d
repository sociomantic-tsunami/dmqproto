/*******************************************************************************

    Forwards queue requests to turtle request implementations
    in turtle.nide.queue.request.*

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.ConnectionHandler;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.util.log.Log;

import ocean.net.server.connection.IConnectionHandler;

import swarm.node.connection.ConnectionHandler;
import dmqproto.client.legacy.DmqConst;

import ocean.transition;

/*******************************************************************************

    Reference to common fakedmq logger instance

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("fakedmq");
}

/*******************************************************************************

    Simple turtle queue connection handler. Implements requests in terms
    of trivial array based storage backend.

*******************************************************************************/

public class DmqConnectionHandler :
    ConnectionHandlerTemplate!(DmqConst.Command)
{
    import ocean.io.select.client.FiberSelectEvent;

    import dmqproto.node.request.model.DmqCommand;

    import fakedmq.request.Pop;
    import fakedmq.request.Push;
    import fakedmq.request.Produce;
    import fakedmq.request.ProduceMulti;
    import fakedmq.request.GetChannels;
    import fakedmq.request.GetChannelSize;
    import fakedmq.request.GetSize;
    import fakedmq.request.GetNumConnections;
    import fakedmq.request.Consume;
    import fakedmq.request.PushMulti;
    import fakedmq.request.RemoveChannel;

    /***************************************************************************

        Provides resources required by the protocol. As this is implementation
        for testing purposes it simply allocates as much stuff as necessary to
        keep code simple.

    ***************************************************************************/

    private scope class DmqRequestResources : DmqCommand.Resources
    {
        import swarm.protocol.StringListReader;

        /***********************************************************************

            Backs all resource getters.

            Struct wrapper is used to workaround D inability to allocate slice
            itself on heap via `new`.

        ***********************************************************************/

        struct Buffer
        {
            mstring data;
        }

        /***********************************************************************

            Used to write channel names to

        ***********************************************************************/

        override public mstring* getChannelBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to write data values to

        ***********************************************************************/

        override public mstring* getValueBuffer ( )
        {
            return &((new Buffer).data);
        }

        /***********************************************************************

            Used to read list of channel names

        ***********************************************************************/

        override public StringListReader getChannelListReader ( )
        {
            return new StringListReader(this.outer.reader, (new Buffer).data);
        }
    }

    /***************************************************************************

        Select event used by some requests to suspend execution until some
        event occurs.

    ***************************************************************************/

    private FiberSelectEvent event;

    /***************************************************************************

        Constructor.

        Params:
            finalize_dg = user-specified finalizer, called when the connection
                is shut down
            setup = struct containing everything needed to set up a connection

    ***************************************************************************/

    public this (void delegate(IConnectionHandler) finalize_dg,
        ConnectionSetupParams setup )
    {
        super(finalize_dg, setup);

        this.event = new FiberSelectEvent(this.writer.fiber);
    }

    /***************************************************************************

        Command code 'None' handler. Treated the same as an invalid command
        code.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        this.handleInvalidCommand();
    }

    /***************************************************************************

        Command code 'Push' handler.

    ***************************************************************************/

    override protected void handlePush ( )
    {
        this.handleCommand!(Push);
    }

    /***************************************************************************

        Command code 'Pop' handler.

    ***************************************************************************/

    override protected void handlePop ( )
    {
        this.handleCommand!(Pop);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        this.handleCommand!(GetChannels);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        this.handleCommand!(GetChannelSize);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        this.handleCommand!(GetSize);
    }


    /***************************************************************************

        Command code 'Consume' handler.

    ***************************************************************************/

    override protected void handleConsume ( )
    {
        this.handleCommand!(Consume);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        this.handleCommand!(GetNumConnections);
    }


    /***************************************************************************

        Command code 'PushMulti' handler.

    ***************************************************************************/

    override protected void handlePushMulti ( )
    {
        this.handleCommand!(PushMulti);
    }


    /***************************************************************************

        Command code 'Produce' handler.

    ***************************************************************************/

    override protected void handleProduce ( )
    {
        this.handleCommand!(Produce);
    }


    /***************************************************************************

        Command code 'ProduceMulti' handler.

    ***************************************************************************/

    override protected void handleProduceMulti ( )
    {
        this.handleCommand!(ProduceMulti);
    }

    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        this.handleCommand!(RemoveChannel);
    }

    /***************************************************************************

        Generic command handler. Creates request-specific handler scope class
        and uses it for actual processing.

    ***************************************************************************/

    private void handleCommand ( Handler : DmqCommand ) ( )
    {
        log.trace("handling {}", Handler.stringof);

        scope resources = new DmqRequestResources;
        scope handler = new Handler(this.reader, this.writer, this.event,
            resources);

        static mstring buffer;
        scope(success)
            log.trace("successfully handled {}", handler.description(buffer));
        scope(failure)
            log.trace("failure while handling {}", handler.description(buffer));

        handler.handle();
    }

    /***************************************************************************

        Called when a connection is finished. Unregisters the reader & writer
        from epoll and closes the connection socket (via
        IConnectionhandler.finalize()).

    ***************************************************************************/

    public override void finalize ( )
    {
        this.writer.fiber.epoll.unregister(this.writer);
        this.writer.fiber.epoll.unregister(this.reader);
        super.finalize();
    }
}
