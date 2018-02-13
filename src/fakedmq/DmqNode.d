/*******************************************************************************

    Provides turtle quenode implementation, used to emulate environment
    for tested applications that work with queue node.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.DmqNode;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.log.Logger;

import fakedmq.ConnectionHandler;

import swarm.node.model.NeoNode;

/*******************************************************************************

    Reference to common fakemq logger instance

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("fakedmq");
}

/*******************************************************************************

    Simple turtle DMQ node. See turtle.node.dmq.ConnectionHandler for more
    implementation details

*******************************************************************************/

public class DmqNode
    : NodeBase!(DmqConnectionHandler)
{
    import ocean.io.Stdout : Stderr;
    import core.stdc.stdlib : abort;

    import ocean.io.select.client.model.ISelectClient : IAdvancedSelectClient;
    import ocean.net.server.connection.IConnectionHandlerInfo;
    import ocean.io.select.protocol.generic.ErrnoIOException;
    import ocean.io.select.client.TimerEvent;

    import dmqproto.client.legacy.DmqConst;
    import swarm.node.connection.ConnectionHandler;
    import swarm.neo.authentication.HmacDef: Key;
    import Neo = swarm.neo.node.ConnectionHandler;
    import fakedmq.neo.RequestHandlers;
    import fakedmq.neo.SharedResources;
    import fakedmq.Storage;
    import swarm.neo.AddrPort;

    /***************************************************************************

        Flag indicating that unhandled exceptions from the node must be printed
        in test suite trace

    ***************************************************************************/

    public bool log_errors = true;

    /***************************************************************************

        Timer for periodic consumer flushing

    ***************************************************************************/

    private TimerEvent flush_timer;

    /***************************************************************************

        Constructor

        Params:
            node_item = node address & port
            epoll = epoll select dispatcher to be used internally

    ***************************************************************************/

    public this ( DmqConst.NodeItem node_item, EpollSelectDispatcher epoll )
    {
        // maximum length for the queue of pending connections
        int backlog = 20;

        auto params = new ConnectionSetupParams;
        params.epoll = epoll;
        params.node_info = this;

        Options neo_options;
        neo_options.requests = request_handlers;
        neo_options.epoll = epoll;
        neo_options.no_delay = true; // favour network turn-around over packet efficiency
        neo_options.credentials_map["test"] = Key.init;

        ushort neo_port = node_item.Port;

        // If original port is 0, operating system should auto-choose the port
        if (neo_port != 0)
        {
            neo_port++;
        }

        super(node_item, neo_port, params, neo_options, backlog);
        this.error_callback = &this.onError;

        this.flush_timer = new TimerEvent(&this.onFlushTimer);
        this.flush_timer.set(1,0,1,0);
        epoll.register(this.flush_timer);
    }

    /***************************************************************************

        Override of standard `stopListener` to also clean fake node consumer
        data in global storage and unregister the flush timer.

    ***************************************************************************/

    override public void stopListener ( EpollSelectDispatcher epoll )
    {
        super.stopListener(epoll);
        global_storage.dropAllConsumers();
        this.flush_timer.set(0,0,0,0);
        epoll.unregister(this.flush_timer);
    }

    /***************************************************************************

        Simple `shutdown` implementation to stop logging unhandled exceptions
        when it is initiated.

    ***************************************************************************/

    override public void shutdown ( )
    {
        this.log_errors = false;
    }

    /***************************************************************************

        Callback for `this.flush_timer`; flushes all consumers.

        Returns:
            true to stay registered with epoll.

    ***************************************************************************/

    private bool onFlushTimer ( )
    {
        global_storage.flushAllConsumers();
        return true;
    }

    /***************************************************************************

        Make any error fatal

    ***************************************************************************/

    private void onError ( Exception exception, IAdvancedSelectClient.Event,
        IConnectionHandlerInfo )
    {
        if (!this.log_errors)
            return;

        .log.warn("Ignoring exception: {} ({}:{})",
            getMsg(exception), exception.file, exception.line);

        // socket errors can be legitimate, for example if client has terminated
        // the connection early
        if (cast(IOWarning) exception)
            return;

        // can be removed in next major version
        version(none)
        {
            // anything else is unexpected, die at once
            abort();
        }
    }

    /***************************************************************************

        Returns:
            identifier string for this node

    ***************************************************************************/

    override protected cstring id ( )
    {
        return "Fake Turtle DMQ Node";
    }

    /***************************************************************************

        Scope allocates a request resource acquirer instance and passes it to
        the provided delegate for use in a request.

        Params:
            handle_request_dg = delegate that receives a resources acquirer and
                initiates handling of a request

    ***************************************************************************/

    override protected void getResourceAcquirer (
        void delegate ( Object request_resources ) handle_request_dg )
    {
        // In the fake node, we don't actually store a shared resources
        // instance; a new one is simply passed to each request.
        handle_request_dg(new SharedResources);
    }
}
