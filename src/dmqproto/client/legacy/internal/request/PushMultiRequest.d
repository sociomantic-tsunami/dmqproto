/*******************************************************************************

    Asynchronously/Selector managed DMQ PushMulti request class

    Copyright:
        Copyright (c) 2010-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.PushMultiRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.model.IMultiChannelRequest;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import swarm.client.ClientExceptions;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import ocean.core.Enforce;




/*******************************************************************************

    PushMultiRequest class

*******************************************************************************/

public scope class PushMultiRequest : IMultiChannelRequest
{
    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resources which might be required by the request

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        IDmqRequestResources resources )
    {
        super(reader, writer, resources);
    }


    /***************************************************************************

        Sends the node any data required by the request.

        The base class has already sent the command and channels, so this
        request needs to send the value to be pushed.

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
        auto input = this.params.io_item.put_value();
        auto value = input(super.params.context);
        enforce(this.resources.empty_value_exception(), value.length);

        this.writer.writeArray(value);
    }


    /***************************************************************************

        Handles a request once the request data has been sent and a non-ok
        (skip) status has been received from the node.

        Informs the client that the request has finished with no error. As this
        skip status can be sent when the push may have succeeded for one channel
        but failed for others we cannot retry the request on any other nodes or
        some data may be replicated on multiple nodes.

    ***************************************************************************/

    override protected void statusActionSkip ( )
    {
        this.finished();
    }


    /***************************************************************************

        Handles a request once the request data has been sent and an error
        status has been received from the node.

        The fatal status is only returned when a node fails to create an empty
        channel. As all the channels are created before records are pushed,
        this status can only be received when NO records have been pushed so it
        is safe to retry the request on another node.

        Tries the request on the next node, or informs the client that the
        request has finished, if all nodes have been tried already.

    ***************************************************************************/

    override protected void statusActionFatal ( )
    {
        with ( RegisterNextResult ) final switch ( this.resources.reregistrator.
            registerNext(this.params) )
        {
            case Reregistered: break;
            case NoMoreNodes:
                this.finished(this.resources.all_nodes_failed_exception()
                    (__FILE__, __LINE__));
                break;
            case MultipleNodeQuery:
                assert(false, "pushMulti: request is not multiple node");
            version (D_Version2) {} else default:
                assert(false, "pushMulti: unknown registerNext response");
        }
    }


    /***************************************************************************

        Handles the request once the request data has been sent and a valid
        status has been received from the node.

        This request needs do nothing here.

    ***************************************************************************/

    override protected void handle__ ( )
    {
        this.finished();
    }


    /***************************************************************************

        Calls the request notification delegate with type GroupFinished.

    ***************************************************************************/

    private void finished ( lazy Exception e = null )
    {
        if ( this.params.notifier !is null )
        {
            scope info = new RequestNotification(
                cast(DmqConst.Command.E)this.params.command,
                this.params.context);
            info.type = info.type.GroupFinished;
            info.exception = e;

            this.params.notifier(info);
        }
    }
}

