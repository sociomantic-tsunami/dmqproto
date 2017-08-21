/*******************************************************************************

    Consume request protocol.

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.SingleChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    Consume request protocol

*******************************************************************************/

public abstract scope class Consume : SingleChannel
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
        super(DmqConst.Command.E.Consume, reader, writer, resources);
    }

    /***************************************************************************

        Keep sending channel records to clients until either client halts
        or channel gets deleted

        Params:
            channel_name = name of channel to be filled with records

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    {
        this.writer.write(DmqConst.Status.E.Ok);

        bool finish, flush;
        auto value_buffer = this.resources.getValueBuffer();

        // "finish" indicates that channel can't be read from anymore

        // in practice request will most often terminate via exception
        // from write method because connection was closed by the client
        while (!finish)
        {
            while (this.getNextValue(channel_name, *value_buffer))
            {
                this.writer.writeArray(*value_buffer);
            }

            this.waitEvents(finish, flush);

            // handle flushing
            if (flush)
            {
                this.flush();
            }
        }

        // Write empty value, informing the client that the request has
        // finished
        this.writer.writeArray("");
    }

    /***************************************************************************

        Called when waitEvents() signals that the write buffer should be
        flushed.

    ***************************************************************************/

    protected void flush ( )
    {
        this.writer.flush();
    }

    /***************************************************************************

        Retrieve next value from the channel if available

        Params:
            channel_name = channel to get value from
            value        = array to write value to

        Returns:
            `true` if there was a value in the channel

    ***************************************************************************/

    abstract protected bool getNextValue ( cstring channel_name, ref mstring value );

    /***************************************************************************

        When there are no more items in the channel, this method is called in
        order to wait for more to arrive. It may also return a notification of
        other types of events via its ref parameters.

        Params:
            finish = indicates if request needs to be ended
            flush =  indicates if socket needs to be flushed

    ***************************************************************************/

    abstract protected void waitEvents ( out bool finish, out bool flush );
}
