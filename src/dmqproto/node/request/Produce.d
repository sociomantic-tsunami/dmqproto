/*******************************************************************************

    Produce request protocol.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.Produce;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.SingleChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    Produce request protocol

*******************************************************************************/

public abstract scope class Produce : SingleChannel
{
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
        super(DmqConst.Command.E.Produce, reader, writer, resources);
    }

    /***************************************************************************

        Reads data for `pushRecord` until client terminates the connection or
        sends an empty record

        Params:
            channel_name = name of channel to be filled with records

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    { 
        this.writer.write(DmqConst.Status.E.Ok);
        this.writer.flush();

        auto value_buffer = this.resources.getValueBuffer();

        while ( true )
        {
            this.reader.readArray(*value_buffer);

            if ( value_buffer.length == 0 )
                break;

            this.pushRecord(channel_name, *value_buffer);
        }
    }

    /***************************************************************************

        Must try pushing received record to the storage channel.
        Failure must be ignored.

        Params:
            channel_name = name of channel to push to
            value = record value to push

    ***************************************************************************/

    abstract protected void pushRecord ( cstring channel_name, cstring value );
}
