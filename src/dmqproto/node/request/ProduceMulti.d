/*******************************************************************************

    ProduceMulti request protocol.

    Copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.ProduceMulti;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.MultiChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    ProduceMulti request protocol

*******************************************************************************/

public abstract scope class ProduceMulti : MultiChannel
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
        super(DmqConst.Command.E.ProduceMulti, reader, writer, resources);
    }

    /***************************************************************************

        Keeps reading records from the client and pushing them to channels
        until client terminates the connection or send an empty value

        Params:
            channel_names = names of channels to be filled with records

    ***************************************************************************/

    override protected void handleMultiChannelRequest ( in cstring[] channel_names )
    {
        this.writer.write(DmqConst.Status.E.Ok);
        this.writer.flush(); // flush write buffer, so client can start sending

        auto value_buffer = this.resources.getValueBuffer();

        while ( true )
        {
            this.reader.readArray(*value_buffer);

            if ( value_buffer.length == 0 )
                break;

            this.pushRecord(channel_names, *value_buffer);
        }
    }

    /***************************************************************************

        To be overriden by derivatives. Must push received record to one or
        more storage channels. Failures are ignored.

        Params:
            channel_names = names of channels to push to
            value = record value to push

    ***************************************************************************/

    abstract protected void pushRecord ( in cstring[] channel_names, cstring value );
}
