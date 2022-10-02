/*******************************************************************************

    RemoveChannel request protocol.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.RemoveChannel;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;

import dmqproto.node.request.model.SingleChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    RemoveChannel request protocol

*******************************************************************************/

public abstract class RemoveChannel : SingleChannel
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
        super(DmqConst.Command.E.RemoveChannel, reader, writer, resources);
    }

    /***************************************************************************

        Make appropriate status response and forward to `removeChannel` to do
        actual work

        Params:
            channel_name = name of channel to be removed

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    {
        this.writer.write(DmqConst.Status.E.Ok);
        this.removeChannel(channel_name);
    }

    /***************************************************************************

        Must remove the specified channel from the storage engine.
        Any failure is considered critical.

        Params:
            channel_name = name of channel to be removed

    ***************************************************************************/

    abstract protected void removeChannel ( cstring channel_name );
}
