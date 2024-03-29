/*******************************************************************************

    Pop request protocol.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.Pop;

import dmqproto.node.neo.request.core.RequestHandler;

/*******************************************************************************

    v0 Pop request protocol.

*******************************************************************************/

public abstract class PopProtocol_v1: RequestHandler
{
    import dmqproto.common.Pop;
    import dmqproto.common.RequestCodes;
    import dmqproto.node.neo.request.core.IRequestResources;

    import swarm.neo.node.RequestOnConn;
    import swarm.neo.request.Command;

    import ocean.meta.types.Qualifiers;

    /// Request name for stats tracking. Required by ConnectionHandler.
    static immutable string name = "pop";

    /// Request code / version. Required by ConnectionHandler.
    static immutable Command command = Command(RequestCode.Pop, 1);

    /// Flag indicating whether timing stats should be gathered for requests of
    /// this type.
    static immutable bool timing = false;

    /// Flag indicating whether this request type is scheduled for removal. (If
    /// true, clients will be warned.)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        Request handler.

        Params:
            connection = connection to client
            resources = request resources acquirer
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    override protected void handle ( RequestOnConn connection,
        IRequestResources resources, const(void)[] msg_payload )
    {
        cstring channel_name;

        this.ed.message_parser.parseBody(msg_payload, channel_name);

        bool subscribed;

        if (this.prepareChannel(resources, channel_name, subscribed))
        {
            auto value = resources.getVoidBuffer();
            if ( this.getNextValue(*value) )
            {
                this.ed.send(
                    ( this.ed.Payload payload )
                    {
                        payload.addCopy(RequestStatusCode.Popped);
                        payload.addArray(*value);
                    }
                );
            }
            else
            {
                this.ed.send(
                    ( this.ed.Payload payload )
                    {
                        payload.addCopy(RequestStatusCode.Empty);
                    }
                );
            }
        }
        else
        {
            this.ed.send(
                ( this.ed.Payload payload )
                {
                    payload.addCopy(
                        subscribed
                        ? RequestStatusCode.Subscribed
                        :RequestStatusCode.Error
                    );
                }
            );
        }
    }

    /***************************************************************************

        Performs any logic needed to pop from the channel of the given name.

        Params:
            resources = request resources
            channel_name = channel to pop from
            subscribed = `true` if the return value is `false` because the
                channel has subscribers so it is not possible to pop from it

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    abstract protected bool prepareChannel ( IRequestResources resources,
        cstring channel_name, out bool subscribed );

    /***************************************************************************

        Pop the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    abstract protected bool getNextValue ( ref void[] value );
}
