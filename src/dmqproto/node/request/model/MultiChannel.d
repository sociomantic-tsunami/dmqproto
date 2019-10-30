/*******************************************************************************

    Abstract base class for DMQ request protocols over a group of channels.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.model.MultiChannel;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;
import swarm.Const : validateChannelName;
import swarm.protocol.StringListReader;

import ocean.transition;

/*******************************************************************************

    DMQ channel request protocol base class

*******************************************************************************/

public abstract scope class MultiChannel : DmqCommand
{
    /***************************************************************************

        Channel list argument (external slice)

    ***************************************************************************/

    protected const(cstring)[] channels;

    /***************************************************************************

        Constructor

        Params:
            cmd = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests

    ***************************************************************************/

    public this ( DmqConst.Command.E cmd, FiberSelectReader reader,
        FiberSelectWriter writer, DmqCommand.Resources resources )
    {
        super(cmd, reader, writer, resources);
    }

    /***************************************************************************

        Reads any data from the client which is required for the request. If the
        request is invalid in some way then the command can be simply ignored
        and all client data has been already read, leaving the read buffer in
        a clean state ready for the next request.

    ***************************************************************************/

    final override protected void readRequestData ( )
    {
        this.channels = this.resources.getChannelListReader.read();
        this.readMultiChannelRequestData();
    }

    /***************************************************************************

        If protocol for derivate request needs any parameters other than
        channel name and request code, this method must be overridden to read
        and store those.

    ***************************************************************************/

    protected void readMultiChannelRequestData ( ) { }

    /***************************************************************************

        Validate arguments (channel names)

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        foreach (channel; this.channels)
        {
            if (!validateChannelName(channel))
            {
                this.writer.write(DmqConst.Status.E.BadChannelName);
                return;
            }
        }

        this.handleMultiChannelRequest(this.channels);
    }

    /***************************************************************************

        Must be overridden by derivative to do actual request handling

        Params:
            channel_names = list of channel names read from client request

    ***************************************************************************/

    abstract protected void handleMultiChannelRequest ( in cstring[] channel_names );
}
