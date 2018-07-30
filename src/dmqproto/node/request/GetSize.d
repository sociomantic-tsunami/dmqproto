/*******************************************************************************

    GetSize request protocol.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.GetSize;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    GetChannels request protocol

*******************************************************************************/

public abstract scope class GetSize : DmqCommand
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
        super(DmqConst.Command.E.GetSize, reader, writer, resources);
    }

    /***************************************************************************

        Payload structs that holds requested metadata 

    ***************************************************************************/

    protected struct SizeData
    {
        mstring address;
        ushort port;
        ulong  records;
        ulong  bytes;
    }

    /***************************************************************************

        No data expected for GetSize request

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
