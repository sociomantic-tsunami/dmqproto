/*******************************************************************************

    Asynchronously/Selector managed DMQ Pop request class

    Processes the DMQ node's output after a Pop command, and forwards the
    received value to the provided output delegate.

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.PopRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.model.IChannelRequest;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import ocean.transition;
import ocean.core.Verify;


/*******************************************************************************

    PopRequest class

*******************************************************************************/

public scope class PopRequest : IChannelRequest
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
        needs send nothing more.

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
    }


    /***************************************************************************

        Handles the request once the request data has been sent and a valid
        status has been received from the node.

        If an empty value is popped from the node, then the reregistrator is
        used to check if further DMQ nodes should be tried. If all nodes have
        already been tried, then there is no data available to pop.

        When a value is received or when all nodes have been tried in vain, the
        finished method is called.

    ***************************************************************************/

    override protected void handle__ ( )
    {
        auto value = this.resources.value_buffer;
        this.reader.readArray(*value);

        if ( value.length > 0 )
        {
            this.finished(value);
        }
        else
        {
            this.params.force_assign = true;

            with ( RegisterNextResult ) final switch ( this.resources.reregistrator.
                registerNext(this.params) )
            {
                case Reregistered: break;
                case MultipleNodeQuery:
                    verify(false, "pop: is not multiple node");
                    assert(false);
                case NoMoreNodes:
                    this.finished(value); break;
                version (D_Version2) {} else default:
                    assert(false, "pop: unknown registerNext response");
            }
        }
    }


    /***************************************************************************

        The output delegate is called with any available data. An empty data
        value represents "DMQ empty". The request notification delegate is
        then called with the type GroupFinished.

        Params:
            value = a pointer to the data read in by the pop query (if any).

    ***************************************************************************/

    private void finished ( mstring* value )
    {
        auto output = this.params.io_item.get_value();
        output(this.params.context, *value);

        if ( this.params.notifier !is null )
        {
            scope info = new RequestNotification(
                cast(DmqConst.Command.E)this.params.command,
                this.params.context);
            info.type = info.type.GroupFinished;

            this.params.notifier(info);
        }
    }
}

