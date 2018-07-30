/*******************************************************************************

    Pop request protocol.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.SingleChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    Pop request protocol

*******************************************************************************/

public abstract scope class Pop : SingleChannel
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
        super(DmqConst.Command.E.Pop, reader, writer, resources);
    }

    /***************************************************************************

        Responds with requested value

        Params:
            channel_name = name of channel to be popped

    ***************************************************************************/

    override protected void handleChannelRequest ( cstring channel_name )
    {
        this.writer.write(DmqConst.Status.E.Ok);
        this.writer.writeArray(this.getNextValue(channel_name));
    }

    /***************************************************************************

        To be implemented by derivates. Must pop last stored value in the
        channel and return it.

        Params:
            channel_name = name of channel to be popped

        Returns:
            popped value, empty array if channel is empty

    ***************************************************************************/

    abstract protected Const!(void)[] getNextValue ( cstring channel_name );
}
