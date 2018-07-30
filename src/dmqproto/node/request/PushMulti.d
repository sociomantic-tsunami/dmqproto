/*******************************************************************************

    PushMulti request protocol.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.PushMulti;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dmqproto.node.request.model.MultiChannel;
import dmqproto.node.request.model.DmqCommand;

import dmqproto.client.legacy.DmqConst;

/*******************************************************************************

    PushMulti request protocol

*******************************************************************************/

public abstract scope class PushMulti : MultiChannel
{
    /***************************************************************************
    
        External buffer where record value get stored to

    ***************************************************************************/

    protected mstring* value_buffer;

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
        super(DmqConst.Command.E.PushMulti, reader, writer, resources);
        this.value_buffer = this.resources.getValueBuffer();
    }

    /***************************************************************************

        Read value to push

    ***************************************************************************/

    override protected void readMultiChannelRequestData ( )
    {
        this.reader.readArray(*this.value_buffer);
    }

    /***************************************************************************

        Process the request. Responds with OK status code only if all records
        have been written successfully. Otherwise error status gets sent.

        Params:
            channel_names = names of channels to be filled with records

    ***************************************************************************/

    override protected void handleMultiChannelRequest ( in cstring[] channel_names )
    {
        auto value = *this.value_buffer;

        if (value.length == 0)
        {
            this.writer.write(DmqConst.Status.E.EmptyValue);
            return;
        }

        if (!this.prepareChannels(channel_names))
        {
            this.writer.write(DmqConst.Status.E.Error);
            return;
        }

        foreach (channel; channel_names)
        {
            this.pushValue(channel, value);
        }

        this.writer.write(DmqConst.Status.E.Ok);
    }

    /***************************************************************************

        To be overriden by derivatives.
        Ensure that requested channels exist / can be created and can be written
        to.

        Params:
            channel_name = list of channel names to checl

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    abstract protected bool prepareChannels ( in cstring[] channel_names );

    /***************************************************************************

        To be overriden by derivatives.
        Push the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

    ***************************************************************************/

    abstract protected void pushValue ( cstring channel_name, in void[] value );
}
