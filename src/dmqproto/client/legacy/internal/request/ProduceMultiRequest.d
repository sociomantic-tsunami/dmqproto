/*******************************************************************************

    Asynchronously/Selector managed DMQ ProduceMulti request class

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.ProduceMultiRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.Const : NodeItem;

import dmqproto.client.legacy.internal.request.model.IMultiChannelRequest;

import dmqproto.client.legacy.internal.request.model.IProducer;

import dmqproto.client.legacy.internal.request.helper.ValueProducer;

import swarm.client.request.model.IStreamInfo;

import ocean.io.select.client.FiberSelectEvent;

import ocean.transition;


/*******************************************************************************

    ProduceMultiRequest class

*******************************************************************************/

public scope class ProduceMultiRequest : IMultiChannelRequest, IStreamInfo
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

        The base class has already sent the command & channels, so this request
        needs do nothing more.

    ***************************************************************************/

    override protected void sendRequestData__ ( )
    {
        this.resources.flushables() += this.resources.value_producer;
    }


    /***************************************************************************

        Handles the request once the request data has been sent and a valid
        status has been received from the node.

    ***************************************************************************/

    override protected void handle__ ( )
    {
        scope ( exit ) this.resources.flushables() -=
            this.resources.value_producer;

        // Pass stream info interface to user.
        if ( this.params.stream_info_register !is null )
        {
            this.params.stream_info_register(this.params.context, this);
        }

        auto ready_for_data = this.params.io_item.producer();

        cstring value;
        do
        {
            value = this.resources.value_producer()(ready_for_data,
                this.params.context);

			this.bytes_handled_ += value.length;

            this.writer.writeArray(value);
        }
        while ( value.length ); // produce can be ended with an empty value

        /*
         * Flush when finished so that the node receives the notification that
         * this ProduceMulti request has ended.
         */

        this.writer.flush();
    }
}

