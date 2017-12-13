/*******************************************************************************

    DMQ client connection handler.

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.connection.DmqRequestConnection;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.connection.RequestConnection;

import swarm.client.request.model.IFlushable : IFlushables;

import swarm.client.ClientExceptions
    : EmptyValueException, FatalErrorException;

import swarm.client.connection.model.INodeConnectionPool;
import swarm.client.connection.model.INodeConnectionPoolInfo;

import dmqproto.client.legacy.DmqConst;

import dmqproto.client.legacy.internal.DmqClientExceptions
    : AllNodesFailedException;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import swarm.client.request.notifier.IRequestNotification;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import dmqproto.client.legacy.internal.connection.SharedResources;

import dmqproto.client.legacy.internal.request.params.RequestParams;

import dmqproto.client.legacy.internal.request.model.IRequest;
import dmqproto.client.legacy.internal.request.model.IChannelRequest;
import dmqproto.client.legacy.internal.request.model.IDmqRequestResources;

import swarm.client.request.GetChannelsRequest;
import swarm.client.request.GetNumConnectionsRequest;
import swarm.client.request.GetChannelSizeRequest;
import swarm.client.request.GetSizeRequest;
import swarm.client.request.RemoveChannelRequest;

import dmqproto.client.legacy.internal.request.PopRequest;
import dmqproto.client.legacy.internal.request.ConsumeRequest;
import dmqproto.client.legacy.internal.request.ProduceRequest;
import dmqproto.client.legacy.internal.request.ProduceMultiRequest;
import dmqproto.client.legacy.internal.request.PushRequest;
import dmqproto.client.legacy.internal.request.PushMultiRequest;

debug ( SwarmClient ) import ocean.io.Stdout;

import ocean.transition;

/*******************************************************************************

    Request classes derived from templates in core

*******************************************************************************/

private alias GetChannelsRequestTemplate!(IRequest,
    IRequest.IDmqRequestResources, DmqConst.Command.E.GetChannels)
    GetChannelsRequest;

private alias GetNumConnectionsRequestTemplate!(IRequest,
    IRequest.IDmqRequestResources, DmqConst.Command.E.GetNumConnections)
    GetNumConnectionsRequest;

private alias GetChannelSizeRequestTemplate!(IChannelRequest,
    IRequest.IDmqRequestResources, DmqConst.Command.E.GetChannelSize)
    GetChannelSizeRequest;

private alias GetSizeRequestTemplate!(IRequest,
    IRequest.IDmqRequestResources, DmqConst.Command.E.GetSize)
    GetSizeRequest;

private alias RemoveChannelRequestTemplate!(IChannelRequest,
    IRequest.IDmqRequestResources, DmqConst.Command.E.RemoveChannel)
    RemoveChannelRequest;



/*******************************************************************************

    DmqRequestConnection

    Provides a DMQ node socket connection and Reqest instances for the DMQ
    requests.

*******************************************************************************/

public class DmqRequestConnection :
    RequestConnectionTemplate!(DmqConst.Command)
{
    /***************************************************************************

        Helper class to acquire and relinquish resources required by a request
        while it is handled. The resources are acquired from the shared
        resources instance which is passed to DmqRequestConnection's
        constructor. Acquired resources are automatically relinquished in the
        destructor.

        Note that it is assumed that each request will own at most one of each
        resource type (it is not possible, for example, to acquire two value
        buffers).

    ***************************************************************************/

    private scope class DmqRequestResources
        : RequestResources, IDmqRequestResources
    {
        import swarm.Const : NodeItem;


        /***********************************************************************

            Constructor.

        ***********************************************************************/

        public this ( )
        {
            super(this.outer.shared_resources);
        }


        /***********************************************************************

            Flushables getter.

        ***********************************************************************/

        public IFlushables flushables ( )
        {
            return this.outer.flushables;
        }


        /***********************************************************************

            Connection pool info getter.

        ***********************************************************************/

        public INodeConnectionPoolInfo conn_pool_info  ( )
        {
            return this.outer.conn_pool;
        }


        /***********************************************************************

            Reregistrator getter.

        ***********************************************************************/

        public IReregistrator reregistrator ( )
        {
            return this.outer.reregistrator;
        }


        /***********************************************************************

            All nodes failed exception getter.

        ***********************************************************************/

        public AllNodesFailedException all_nodes_failed_exception ( )
        {
            return this.outer.all_nodes_failed_exception;
        }


        /***********************************************************************

            Invalid status exception getter.

        ***********************************************************************/

        public FatalErrorException fatal_error_exception ( )
        {
            return this.outer.fatal_error_exception;
        }


        /***********************************************************************

            Empty value exception getter.

        ***********************************************************************/

        public EmptyValueException empty_value_exception ( )
        {
            return this.outer.empty_value_exception;
        }


        /***********************************************************************

            Channel buffer newer.

        ***********************************************************************/

        override protected mstring new_channel_buffer ( )
        {
            return new char[10];
        }


        /***********************************************************************

            Value buffer newer.

        ***********************************************************************/

        override protected mstring new_value_buffer ( )
        {
            return new char[50];
        }


        /***********************************************************************

            Address buffer newer.

        ***********************************************************************/

        override protected mstring new_address_buffer ( )
        {
            return new char[15]; // e.g. 255.255.255.255
        }


        /***********************************************************************

            String list reader newer.

            Note: the string list reader returned by this method also acquires
            and uses a channel buffer. It is thus not possible to use the
            channel buffer independently.

        ***********************************************************************/

        override protected StringListReader new_string_list_reader ( )
        {
            this.channel_buffer();
            return new StringListReader(this.outer.reader,
                this.acquired.channel_buffer);
        }


        /***********************************************************************

            Select event newer.

        ***********************************************************************/

        override protected FiberSelectEvent new_event ( )
        {
            return new FiberSelectEvent(this.outer.fiber);
        }


        /***********************************************************************

            Loop ceder newer.

        ***********************************************************************/

        override protected LoopCeder new_loop_ceder ( )
        {
            return new LoopCeder(this.event);
        }


        /***********************************************************************

            Request suspender newer.

        ***********************************************************************/

        override protected RequestSuspender new_request_suspender ( )
        {
            return new RequestSuspender(this.event,
                NodeItem(this.outer.conn_pool.address, this.outer.conn_pool.port),
                this.outer.params.context);
        }


        /***********************************************************************

            Value producer newer.

        ***********************************************************************/

        override protected ValueProducer new_value_producer ( )
        {
            return new ValueProducer(this.outer.writer, this.event,
                 NodeItem(this.outer.conn_pool.address, this.outer.conn_pool.port),
                 this.outer.params.context);
        }


        /***********************************************************************

            String list reader initialiser.

            Note: the string list reader returned by this method also acquires
            and uses a channel buffer. It is thus not possible to use the
            channel buffer independently.

        ***********************************************************************/

        override protected void init_string_list_reader ( StringListReader
            string_list_reader )
        {
            this.channel_buffer();
            string_list_reader.reinitialise(this.outer.reader,
                &this.acquired.channel_buffer);
        }


        /***********************************************************************

            Select event initialiser.

        ***********************************************************************/

        override protected void init_event ( FiberSelectEvent event )
        {
            event.fiber = this.outer.fiber;
        }


        /***********************************************************************

            Loop ceder initialiser.

        ***********************************************************************/

        override protected void init_loop_ceder ( LoopCeder loop_ceder )
        {
            loop_ceder.event = this.event;
        }


        /***********************************************************************

            Request suspender initialiser.

        ***********************************************************************/

        override protected void init_request_suspender
            ( RequestSuspender request_suspender )
        {
            request_suspender.event = this.event;
            request_suspender.nodeitem_ =
                NodeItem(this.outer.conn_pool.address, this.outer.conn_pool.port);
            request_suspender.context_ = this.outer.params.context;
        }


        /***********************************************************************

            Value producer initialiser.

        ***********************************************************************/

        override protected void init_value_producer
            ( ValueProducer value_producer )
        {
            value_producer.writer = this.outer.writer;
            value_producer.event = this.event;
            value_producer.nodeitem_ =
                NodeItem(this.outer.conn_pool.address, this.outer.conn_pool.port);
            value_producer.context_ = this.outer.params.context;
        }
    }


    /***************************************************************************

        Reference to shared resources manager.

    ***************************************************************************/

    private SharedResources shared_resources;


    /***************************************************************************

        Reregistrtor instance, used by requests which need to re-register
        themselves for a series of DMQ nodes until the request succeeds or all
        nodes have been tried.

    ***************************************************************************/

    private IReregistrator reregistrator;


    /***************************************************************************

        Set of requests to be flushed by the Flush client command.

    ***************************************************************************/

    private IFlushables flushables;


    /***************************************************************************

        Re-usable exception instances for various request handling errors.
        Requests can access these via the getters in DmqRequestResources,
        above.

        TODO: these could probably be shared at a higher level, we probably
        don't need one instance per connection.

    ***************************************************************************/

    private AllNodesFailedException all_nodes_failed_exception;
    private FatalErrorException fatal_error_exception;
    private EmptyValueException empty_value_exception;


    /***************************************************************************

        Constructor

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            conn_pool = interface to an instance of NodeConnectionPool which
                handles assigning new requests to this connection, and recycling
                it when finished
            reregistrator = reregistrator instance
            flushables = registry of flushable requests
            params = request params instance used internally to store the
                params for the request currently being handled by this
                connection
            fiber_stack_size = size of connection fibers' stack (in bytes)
            shared_resources = reference to shared resources manager

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, INodeConnectionPool conn_pool,
        IReregistrator reregistrator, IFlushables flushables,
        IRequestParams params, size_t fiber_stack_size,
        SharedResources shared_resources )
    {
        this.reregistrator = reregistrator;
        this.flushables = flushables;
        this.shared_resources = shared_resources;

        this.all_nodes_failed_exception = new AllNodesFailedException;
        this.fatal_error_exception = new FatalErrorException;
        this.empty_value_exception = new EmptyValueException;

        super(epoll, conn_pool, params, fiber_stack_size);
    }


    /***************************************************************************

        Command code 'None' handler.

    ***************************************************************************/

    override protected void handleNone ( )
    {
        assert(false, "Handling command with code None");
    }


    /***************************************************************************

        Command code 'Push' handler.

    ***************************************************************************/

    override protected void handlePush ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(PushRequest)(resources);
    }


    /***************************************************************************

        Command code 'Pop' handler.

    ***************************************************************************/

    override protected void handlePop ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(PopRequest)(resources);
    }


    /***************************************************************************

        Command code 'GetChannels' handler.

    ***************************************************************************/

    override protected void handleGetChannels ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(GetChannelsRequest)(resources);
    }


    /***************************************************************************

        Command code 'GetChannelSize' handler.

    ***************************************************************************/

    override protected void handleGetChannelSize ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(GetChannelSizeRequest)(resources);
    }


    /***************************************************************************

        Command code 'GetSize' handler.

    ***************************************************************************/

    override protected void handleGetSize ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(GetSizeRequest)(resources);
    }


    /***************************************************************************

        Command code 'Consume' handler.

    ***************************************************************************/

    override protected void handleConsume ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(ConsumeRequest)(resources);
    }


    /***************************************************************************

        Command code 'GetNumConnections' handler.

    ***************************************************************************/

    override protected void handleGetNumConnections ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(GetNumConnectionsRequest)(resources);
    }


    /***************************************************************************

        Command code 'PushMulti' handler.

    ***************************************************************************/

    override protected void handlePushMulti ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(PushMultiRequest)(resources);
    }


    /***************************************************************************

        Command code 'Produce' handler.

    ***************************************************************************/

    override protected void handleProduce ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(ProduceRequest)(resources);
    }


    /***************************************************************************

        Command code 'ProduceMulti' handler.

    ***************************************************************************/

    override protected void handleProduceMulti ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(ProduceMultiRequest)(resources);
    }


    /***************************************************************************

        Command code 'RemoveChannel' handler.

    ***************************************************************************/

    override protected void handleRemoveChannel ( )
    {
        scope resources = new DmqRequestResources;
        this.handleCommand!(RemoveChannelRequest)(resources);
    }


    /***************************************************************************

        If the exception is not null (i.e. there has been a socket error when
        trying to complete the operation on this node) and the request
        is a single noe query that should be re-registered, then try and
        execute the operation on any other DMQ nodes when using multiple
        DMQ nodes.

        If there are no other DMQ nodes to try (or we have tried them all),
        call the notifier with the GroupFinished type.

    ***************************************************************************/

    protected override void nextRequest_ ( )
    {
        if ( this.exception !is null )
        {
            auto dmq_params = cast(RequestParams)this.params;

            with ( RegisterNextResult ) switch ( this.reregistrator.
                registerNext(dmq_params) )
            {
                case MultipleNodeQuery: break;
                case NoMoreNodes:
                    debug ( SwarmClient ) Stderr.formatln(
                        "[{}:{}.{}]: nextRequest_ no more nodes to try",
                        this.conn_pool.address,
                        this.conn_pool.port, this.object_pool_index);

                    if ( dmq_params.notifier !is null )
                    {
                        scope info = new RequestNotification(
                            cast(DmqConst.Command.E)dmq_params.command,
                            dmq_params.context);
                        info.type = info.type.GroupFinished;
                        info.exception = this.exception;

                        dmq_params.notifier(info);
                    }
                    break;
                case Reregistered:
                    debug ( SwarmClient ) Stderr.formatln(
                        "[{}:{}.{}]: nextRequest_ {}", this.conn_pool.address,
                        this.conn_pool.port, this.object_pool_index,
                        this.exception.toString());
                    break;
                default:
                    assert(false, "unknown registerNext response");
            }
        }
    }
}
