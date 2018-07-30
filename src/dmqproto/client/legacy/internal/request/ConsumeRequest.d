/*******************************************************************************

    Asynchronously/Selector managed DMQ Consume request class

    Processes the DMQ node's output after a Consume command, and forwards the
    received values to the provided output delegate.

    Copyright:
        Copyright (c) 2010-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.ConsumeRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.Const : NodeItem;

import dmqproto.client.legacy.internal.request.model.IChannelRequest;

import swarm.common.request.helper.LoopCeder;

import swarm.client.request.helper.RequestSuspender;

import swarm.client.request.model.IStreamInfo;

import ocean.io.select.client.FiberSelectEvent;




/*******************************************************************************

    ConsumeRequest class

*******************************************************************************/

public scope class ConsumeRequest : IChannelRequest, IStreamInfo
{
    /***************************************************************************

        Total bytes handled by this request.

    ***************************************************************************/

    private size_t bytes_handled_;


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

        Returns:
            the number of bytes sent/received by the stream (we currently assume
            that a stream request is either sending or receiving)

    ***************************************************************************/

    override public size_t bytes_handled ( )
    {
        return this.bytes_handled_;
    }


    /***************************************************************************

        Returns:
            the nodeitem this producer is associated with

    ***************************************************************************/

    override public NodeItem nodeitem ( )
    {
        return NodeItem(this.resources.conn_pool_info.address,
            this.resources.conn_pool_info.port);
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

    ***************************************************************************/

    override protected void handle__ ( )
    {
        // Pass suspendable interface to user.
        if ( this.params.suspend_register !is null )
        {
            this.params.suspend_register(this.params.context,
                this.resources.request_suspender);
        }

        this.resources.request_suspender.start();

        // Pass stream info interface to user.
        if ( this.params.stream_info_register !is null )
        {
            this.params.stream_info_register(this.params.context, this);
        }

        // Get output delegate
        auto output = this.params.io_item.get_value();

        auto value = this.resources.value_buffer();
        while (true)
        {
            // Read value
            this.reader.readArray(*value);

            // Empty value indicates end of request
            if (!value.length)
                break;

            this.bytes_handled_ += value.length;

            // Forward value
            output(this.params.context, *value);

            // Suspend, if requested
            auto suspended = this.resources.request_suspender.handleSuspension();
            if ( !suspended )
            {
                this.resources.loop_ceder.handleCeding();
            }
        }
    }
}

