/*******************************************************************************

    GetChannelSize request protocol.

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.GetChannelSize;
import dmqproto.node.request.model.DmqCommand;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.SingleChannel;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    RemoveChannel request protocol

*******************************************************************************/

public abstract scope class GetChannelSize : SingleChannel
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
        super(DmqConst.Command.E.GetChannelSize, reader, writer, resources);
    }

    /***************************************************************************

        Replies with ChannelSizeData content as appropriate

        Params:
            channel_name = name of channel to be queried

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    {
        this.writer.write(DmqConst.Status.E.NotSupported);
    }
}
