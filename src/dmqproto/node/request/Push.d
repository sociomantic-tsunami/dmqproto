/*******************************************************************************

    Push request protocol.

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.Push;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.SingleChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    Push request protocol

*******************************************************************************/

public abstract scope class Push : SingleChannel
{
    /***************************************************************************
    
        External buffer where record value get stored to

    ***************************************************************************/

    protected mstring* value_buffer;

    /***************************************************************************

        Constructor

        Params:
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests
            resources = shared resource getters

    ***************************************************************************/

    public this ( FiberSelectReader reader, FiberSelectWriter writer,
        DmqCommand.Resources resources )
    {
        super(DmqConst.Command.E.Push, reader, writer, resources);
        this.value_buffer = this.resources.getValueBuffer();
    }

    /***************************************************************************

        Read value to push

    ***************************************************************************/

    override protected void readChannelRequestData ( )
    {
        this.reader.readArray(*this.value_buffer);
    }

    /***************************************************************************

        Tries to push received value and sends a response code indicating
        whether the attempt was successful.

        Params:
            channel_name = name of channel to be pushed to

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    {
        if ((*this.value_buffer).length == 0)
        {
            this.writer.write(DmqConst.Status.E.EmptyValue);
            return;
        }

        this.pushValue(channel_name, *this.value_buffer);
        this.writer.write(DmqConst.Status.E.Ok);
    }

    /***************************************************************************

        Pushes the value to the storage channel

        Params:
            channel_name = name of channel to be pushed to
            value        = value to write

    ***************************************************************************/

    abstract protected void pushValue ( cstring channel_name, in void[] value );
}
