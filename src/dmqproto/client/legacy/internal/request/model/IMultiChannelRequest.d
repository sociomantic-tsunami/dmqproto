/*******************************************************************************

    Abstract base class for DMQ client requests over multiple channels.

    Copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.model.IMultiChannelRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.model.IRequest;



/*******************************************************************************

    IMultiChannelRequest abstract class

*******************************************************************************/

public abstract scope class IMultiChannelRequest : IRequest
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

        The base class only sends the channels (the command has been written by
        the super class), and calls the abstract sendRequestData__(), which
        sub-classes must implement.

    ***************************************************************************/

    final override protected void sendRequestData_ ( )
    {
        foreach ( channel; this.params.channels )
        {
            this.writer.writeArray(channel);
        }
        this.writer.writeArray(""); // end of list

        this.sendRequestData__();
    }

    abstract protected void sendRequestData__ ( );
}

