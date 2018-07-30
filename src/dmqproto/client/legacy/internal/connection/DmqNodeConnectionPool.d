/*******************************************************************************

    Pool of DMQ node socket connections holding IRequest instances

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.connection.DmqNodeConnectionPool;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.connection.NodeConnectionPool;
import swarm.client.connection.RequestOverflow;

import swarm.client.model.ClientSettings;

import swarm.client.request.model.IFlushable : IFlushables;

import swarm.client.request.notifier.IRequestNotification;

import swarm.client.ClientExceptions: RequestQueueFullException;

import swarm.Const;

import dmqproto.client.legacy.DmqConst;

import dmqproto.client.legacy.internal.connection.SharedResources;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import dmqproto.client.legacy.internal.request.params.RequestParams;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import dmqproto.client.legacy.internal.connection.DmqRequestConnection;

import dmqproto.client.legacy.internal.request.model.IRequest;

import ocean.core.TypeConvert : downcast;

import ocean.transition;
import ocean.core.Verify;

/*******************************************************************************

    DmqNodeConnectionPool

    Provides a pool of DMQ node socket connections where each connection
    instance holds Reqest instances for the DMQ requests.

    TODO: think about using a setup params class for this class, to reduce the
    number of ctor parameters.

*******************************************************************************/

public class DmqNodeConnectionPool : NodeConnectionPool
{
    /***************************************************************************

        Shared resources instance.

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

        Constructor

        Params:
            settings = client settings instance
            epoll = selector dispatcher instances to register the socket and I/O
                events
            address = node address
            port = node service port
            reregistrator = reregistrator instance
            flushables = registry of flushable requests
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            shared_resources = shared resources instance
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( ClientSettings settings, EpollSelectDispatcher epoll,
        mstring address, ushort port, IReregistrator reregistrator,
        IFlushables flushables, IRequestOverflow request_overflow,
        SharedResources shared_resources,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        this.flushables = flushables;
        this.reregistrator = reregistrator;
        this.shared_resources = shared_resources;

        super(settings, epoll, address, port, request_overflow, error_reporter);
    }


    /***************************************************************************

        Creates a new instance of the connection request handler class.

        Returns:
            new DmqRequestConnection instance

    ***************************************************************************/

    override protected DmqRequestConnection newConnection ( )
    {
        return new DmqRequestConnection(this.epoll, this, this.reregistrator,
            this.flushables, this.newRequestParams(),
            this.fiber_stack_size, this.shared_resources);
    }


    /***************************************************************************

        Creates a new instance of the connection request params class.

        Returns:
            new RequestParams instance

    ***************************************************************************/

    override protected IRequestParams newRequestParams ( )
    {
        return new RequestParams;
    }


    /***************************************************************************

        Called if a request was assigned to a node but couldn't be started
        immediately. Reassigns the request to another node if possible.

        Note: This method can be called recursively for all available nodes.

        Params:
            params = request parameters

    ***************************************************************************/

    override protected void queueRequest ( IRequestParams iparams )
    {
        auto params = downcast!(RequestParams)(iparams);
        verify(params !is null);

        if (params.force_assign)
        {
            super.queueRequest(params);
        }
        else
        {
            /*
             * Recursive call: This method is called from super.assign(), and
             * this.reregistrator.registerNext() can call super.assign() to try
             * another node.
             */

            final switch (this.reregistrator.registerNext(params))
            {
                case RegisterNextResult.Reregistered,
                     RegisterNextResult.NoMoreNodes:
                    /*
                     * registerNext() returns
                     *   - Reregistered to indicate it assigned the request to a
                     *     node that was able to start it immediately,
                     *   - NoMoreNodes to indicate that it has tried all nodes
                     *     but no node could start the request immediately so it
                     *     assigned the request to the node with the least full
                     *     request queue by calling forceQueueRequest().
                     */
                    break;

                case RegisterNextResult.MultipleNodeQuery:
                    super.queueRequest(params);
                    break;

                version (D_Version2) {} else default:
                    assert(false, "invalid RegisterNextResult");
            }
        }
    }

    /***************************************************************************

        In case of request queue overflow on the last node tried, call the user
        notifier with GroupFinished, too, as the request will ultimately fail
        then.

        Params:
            iparams = request parameters

    ***************************************************************************/

    override protected void notifyRequestQueueOverflow ( IRequestParams iparams )
    {
        super.notifyRequestQueueOverflow(iparams); // Finished notification

        auto params = downcast!(RequestParams)(iparams);
        verify(params !is null);

        if ( params.force_assign )
        {
           /*
            * GroupFinished notification; this request has ultimately failed.
            *
            * Note that params.force_assign can be true only for single-node
            * requests. It is set in
            *  - DmqNodeRegistry.registerNext(), which doesn't set it for multi-
            *    node requests and
            *  - PopRequest.handle__(), which is a single-node.request.
            */
            params.notify(this.address, this.port,
                this.request_queue_full_exception, IStatusCodes.E.Undefined,
                IRequestNotification.Type.GroupFinished);
        }
    }
}
