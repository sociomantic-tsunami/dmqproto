/*******************************************************************************

    DMQ node connection registry

    Push and pop commands have a special node-cycling behaviour in the DMQ
    client, so that each subsequent request is pushed / popped on the next node
    in a round-robin system. This has the result that data is distributed evenly
    across all DMQ nodes.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.registry.DmqNodeRegistry;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.registry.NodeRegistry;
import swarm.client.connection.NodeConnectionPool;
import swarm.client.connection.RequestOverflow;

import swarm.client.ClientCommandParams;

import dmqproto.client.legacy.internal.registry.model.IDmqNodeRegistry;
import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import dmqproto.client.legacy.internal.connection.SharedResources;

import dmqproto.client.legacy.internal.connection.DmqNodeConnectionPool;
import dmqproto.client.legacy.internal.connection.DmqRequestConnection;

import dmqproto.client.legacy.internal.request.params.RequestParams;

import dmqproto.client.legacy.DmqConst;

import ocean.core.TypeConvert : castFrom, downcast;

debug ( SwarmClient ) import ocean.io.Stdout;

import ocean.transition;
import ocean.core.Verify;

/******************************************************************************

    DmqNodeRegistry

    Registry of DMQ node socket connection pools with one connection pool for
    each DMQ node.

*******************************************************************************/

public class DmqNodeRegistry : NodeRegistry, IDmqNodeRegistry, IReregistrator
{
    /***************************************************************************

        Number of expected nodes in the registry. Used to initialise the
        registry's hash map.

    ***************************************************************************/

    private static immutable expected_nodes = 100;


    /***************************************************************************

        Shared resources instance. Owned by this class and passed to all node
        connection pools.

    ***************************************************************************/

    private SharedResources shared_resources;


    /***************************************************************************

        Indices of the next node to which a push / pop command will be sent (see
        comment in module header).

    ***************************************************************************/

    private uint push_node;

    private uint pop_node;


    /***************************************************************************

        Constructor

        Params:
            epoll = selector dispatcher instance to register the socket and I/O
                events
            settings = client settings instance
            request_overflow = overflow handler for requests which don't fit in
                the request queue
            error_reporter = error reporter instance to notify on error or
                timeout

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ClientSettings settings,
        IRequestOverflow request_overflow,
        INodeConnectionPoolErrorReporter error_reporter )
    {
        verify(epoll !is null, typeof(this).stringof ~ ".ctor: epoll must be non-null");

        super(epoll, settings, request_overflow,
            new NodeSet(this.expected_nodes), error_reporter);

        this.shared_resources = new SharedResources;
    }


    /***************************************************************************

        IReregistrator interface method.

        Adds the specified request to the next DMQ node according to the node
        ID counter of params, if there are nodes which have not been queried
        by previous calls with this particular request.

        Params:
            params = request parameters

        Returns:
            Reregistered if params has been assigned to the next node or
            NoMoreNodes if, according to the node ID counter of request_item,
            all nodes have been queried by previous calls with this particular
            request, or MultipleNodeQuery if this request is a multiple node
            request and should not be re-registered.

    ***************************************************************************/

    public RegisterNextResult registerNext ( RequestParams params )
    {
        if ( !this.allNodesCommand(params.command) )
        {
            bool finished = params.node_id.next(castFrom!(size_t).to!(uint)(
                super.nodes.list.length)).finished;

            if ( !finished )
            {
                debug ( SwarmClient ) Stderr.formatln("registerNext: assign to next node");
                this.currentNode(params).assign(params);
                return RegisterNextResult.Reregistered;
            }
            else
            {
                // If this request has been locked to only be assigned to this
                // node, we cannot reassign it here.
                if (params.force_assign)
                    return RegisterNextResult.NoMoreNodes;

                // Otherwise (the request has *not* been locked to only be
                // assigned to a particular node), we reassign it to the node
                // with the least full request queue and lock it to that node
                // (so it won't be reassigned by future calls of this method).
                params.force_assign = true;
                this.getPoolWithLowestQueueUsage(params.node_id.current).assign(params);
                return RegisterNextResult.Reregistered;
            }
        }

        return RegisterNextResult.MultipleNodeQuery;
    }


    /***************************************************************************

        Determines whether the given request params describe a request which
        should be sent to all nodes simultaneously.

        Params:
            params = request parameters

        Returns:
            true if the request should be added to all nodes

    ***************************************************************************/

    override public bool allNodesRequest ( IRequestParams params )
    {
        return this.allNodesCommand(params.command) && !params.node.set();
    }


    /***************************************************************************

        Creates a new instance of the DMQ node request pool class.

        Params:
            address = node address
            port = node service port

        Returns:
            new NodeConnectionPool instance

    ***************************************************************************/

    override protected NodeConnectionPool newConnectionPool ( mstring address, ushort port )
    {
        return new DmqNodeConnectionPool(this.settings, this.epoll,
            address, port, this, this.flushables, this.request_overflow,
            this.shared_resources, this.error_reporter);
    }


    /***************************************************************************

        Gets the connection pool which is responsible for the given request.
        Push and pop requests are cycled between nodes for even data /
        bandwidth distribution.

        Params:
            params = request parameters

        Returns:
            connection pool responsible for request (null if none found)

    ***************************************************************************/

    override protected NodeConnectionPool getResponsiblePool ( IRequestParams params )
    {
        if ( params.node.set() )
        {
            auto pool = super.inRegistry(params.node.Address, params.node.Port);
            return pool is null ? null : *pool;
        }

        auto dmq_params = cast(RequestParams)params;

        with ( DmqConst.Command.E ) switch ( params.command )
        {
            case Push:
            case PushMulti:
                dmq_params.node_id = this.nextPool(this.push_node);
                break;

            case Pop:
                dmq_params.node_id = this.nextPool(this.pop_node);
                break;

            default:
                verify(false, "invalid command");
        }

        return this.currentNode(dmq_params);
    }


    /***************************************************************************

        Cycles the given index to the next node in the registry.

        Params:
            index = index to be updated, shifted to the index of the next pool
                in super.connection_pools

        Returns:
            original value of index

    ***************************************************************************/

    private uint nextPool ( ref uint index )
    {
        verify(index < super.nodes.list.length, typeof (this).stringof ~ ".getPool - index out of range");

        auto pool = index;
        if ( ++index >= super.nodes.list.length )
        {
            index = 0;
        }
        return pool;
    }


    /***************************************************************************

        Gets the connection pool for the node which the specified request params
        is supposed to access.

        Params:
            params = request params

        Returns:
            connection pool corresponding to the request params' current node

    ***************************************************************************/

    private NodeConnectionPool currentNode ( RequestParams params )
    {
        return this.nodes.list[params.node_id.current];
    }


    /***************************************************************************

        Iterates over all nodes starting with start and returns the node with
        the least amount of bytes in the request queue. If multiple nodes have
        the same amount of bytes in the request queue then the first in the list
        of nodes is returned.

        Params:
            start = start index

        Returns:
            the node with the least amount of bytes in the request queue.

        In:
            start must be less than the number of nodes.

        Out:
            the returned node is valid.

    ***************************************************************************/

    private DmqNodeConnectionPool getPoolWithLowestQueueUsage ( uint start )
    out (node)
    {
        assert(node);
        debug ( SwarmClient )
        {
            Stdout.formatln("getPoolWithLowestQueueUsage({}) -- {}:{}", start, node.address, node.port).flush();
        }
    }
    body
    {
        verify(start < this.nodes.list.length);

        auto pos = start;
        auto selected_node = this.nodes.list[this.nextPool(pos)];
        auto selected_queued = selected_node.queued_bytes + selected_node.overflowed_bytes;

        while (pos != start)
        {
            auto node = this.nodes.list[this.nextPool(pos)];
            auto queued = node.queued_bytes + node.overflowed_bytes;
            if (queued < selected_queued)
            {
                selected_node   = node;
                selected_queued = queued;
            }
        }

        return downcast!(DmqNodeConnectionPool)(selected_node);
    }


    /***************************************************************************

        Checks if the request identified by command should be sent to all nodes
        or a single node.

        Params:
            command = request command

        Returns:
            true if the request should be sent to all nodes or false if it
            should be sent to a single node.

    ***************************************************************************/

    static private bool allNodesCommand ( uint command )
    {
        with ( DmqConst.Command.E ) final switch ( command )
        {
            // Commands over all nodes
            case RemoveChannel:
            case GetNumConnections:
            case Consume:
            case Produce:
            case ProduceMulti:
                return true;

            // Commands over a single node
            case Push:
            case PushMulti:
            case Pop:
                return false;
        }
    }
}
