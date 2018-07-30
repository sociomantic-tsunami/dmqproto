/*******************************************************************************

    Abstract base class for DMQ client requests over a channel.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.model.IChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.model.IRequest;

import dmqproto.client.legacy.DmqConst;

import dmqproto.client.legacy.internal.request.params.RequestParams;




/*******************************************************************************

    IChannelRequest abstract class

*******************************************************************************/

public scope class IChannelRequest : IRequest
{
    /***************************************************************************

        Constructor.

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

        Sends the node any data required by the request.

        The base class only sends the channel (the command has been written by
        the super class), and calls the abstract sendRequestData__(), which
        sub-classes must implement.

    ***************************************************************************/

    final override protected void sendRequestData_ ( )
    {
        this.writer.writeArray(super.params.channel);

        this.sendRequestData__();
    }

    abstract protected void sendRequestData__ ( );
}

