/*******************************************************************************

    Push request protocol.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.Push;

import dmqproto.node.neo.request.core.IRequestHandlerRequest;

/*******************************************************************************

    v2 Push request protocol.

*******************************************************************************/

public abstract class PushProtocol_v2: IRequestHandlerRequest
{
    import dmqproto.common.Push;

    import swarm.neo.util.VoidBufferAsArrayOf;

    import ocean.transition;

    /***************************************************************************

        Request handler.

        Params:
            connection = connection to client
            resources = request resources acquirer
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    override protected void handle ( RequestOnConn connection,
        IRequestResources resources, Const!(void)[] msg_payload )
    {
        auto ed = connection.event_dispatcher;
        auto parser = ed.message_parser;

        // Acquire a buffer to contain slices to the channel names in the
        // message payload (i.e. not a buffer of buffers, a buffer of slices)
        auto channel_names = VoidBufferAsArrayOf!(cstring)(resources.getVoidBuffer());
        channel_names.length = *parser.getValue!(ubyte)(msg_payload);

        foreach ( ref channel_name; channel_names.array )
        {
            channel_name = parser.getArray!(Const!(char))(msg_payload);
        }

        Const!(void)[] value;
        parser.parseBody(msg_payload, value);

        if ( this.prepareChannels(channel_names.array) )
        {
            foreach ( channel_name; channel_names.array )
            {
                this.pushToStorage(channel_name, value);
            }

            ed.send(
                ( ed.Payload payload )
                {
                    payload.addCopy(RequestStatusCode.Pushed);
                }
            );
        }
        else
        {
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addCopy(RequestStatusCode.Error);
                }
            );
        }
    }

    /***************************************************************************

        Ensures that requested channels exist / can be created and can be
        written to.

        Params:
            channel_names = list of channel names to check

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    abstract protected bool prepareChannels ( in cstring[] channel_names );

    /***************************************************************************

        Push a record to the specified storage channel.

        Params:
            channel_name = channel to push to
            value = record value to push

        Returns:
            true if the record was pushed to the channel, false if it failed

    ***************************************************************************/

    abstract protected bool pushToStorage ( cstring channel_name,
        in void[] value );
}
