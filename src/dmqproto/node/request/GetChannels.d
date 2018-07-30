/*******************************************************************************

    GetChannels request protocol. Not supported anymore.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.GetChannels;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    GetChannels request protocol

*******************************************************************************/

public abstract scope class GetChannels : DmqCommand
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
        super(DmqConst.Command.E.GetChannels, reader, writer, resources);
    }

    /***************************************************************************

        No data expected for GetChannels request

    ***************************************************************************/

    final override protected void readRequestData ( ) { }

    /***************************************************************************

        Write status and response data

    ***************************************************************************/

    final override protected void handleRequest ( )
    {
        this.writer.write(DmqConst.Status.E.NotSupported);
    }
}
