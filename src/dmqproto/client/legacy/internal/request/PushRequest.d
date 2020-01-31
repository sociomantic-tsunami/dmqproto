/*******************************************************************************

    Asynchronously/Selector managed DMQ Push request class

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.PushRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.model.IChannelRequest;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import dmqproto.client.legacy.internal.DmqClientExceptions;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import swarm.client.ClientExceptions;

import ocean.core.Enforce;
import ocean.core.Verify;




/*******************************************************************************

    PushRequest class

*******************************************************************************/

public scope class PushRequest : IChannelRequest
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

        The base class has already sent the command & channel, so this request
        just needs to send the value to be pushed.

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
        auto input = this.params.io_item.put_value();

        auto value = input(this.params.context);
        enforce(this.resources.empty_value_exception(), value.length);

        this.writer.writeArray(value);
    }


    /***************************************************************************

        Handles the request once the request data has been sent and a valid
        status has been received from the node.

        Calls the request notification delegate with type GroupFinished to
        indicate that the request is complete.

    ***************************************************************************/

    override protected void handle__ ( )
    {
        this.finished();
    }


    /***************************************************************************

        Handles a request once the request data has been sent and a non-ok
        (skip) status has been received from the node.

        Tries the request on the next node, or informs the client that the
        request has finished, if all nodes have been tried already.

    ***************************************************************************/

    override protected void statusActionSkip ( )
    {
        this.tryNextNode();
    }


    /***************************************************************************

        Handles a request once the request data has been sent and an error
        status has been received from the node.

        Tries the request on the next node, or informs the client that the
        request has finished, if all nodes have been tried already.

    ***************************************************************************/

    override protected void statusActionFatal ( )
    {
        this.tryNextNode();
    }


    /***************************************************************************

        Tries this request again on the next available node. If this request has
        already been tried on all nodes, then the request notification delegate
        is called with type GroupFinished and an exception to indicate that the
        request is complete.

    ***************************************************************************/

    private void tryNextNode ( )
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
                verify(false, "push: is not multiple node");
                assert(false);
        }
    }


    /***************************************************************************

        Calls the request notification delegate with type GroupFinished and an
        optional exception.

        Params:
            e = exception to pass to notification delegate

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

