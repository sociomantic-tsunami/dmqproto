/*******************************************************************************

    Fake DMQ node Push request implementation.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.request.Push;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.neo.request.core.IRequestResources;
import dmqproto.node.neo.request.Push;

import fakedmq.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
    Command.Version cmdver, Const!(void)[] msg_payload )
{
    auto resources = new SharedResources;

    switch ( cmdver )
    {
        case 2:
            scope rq = new PushImpl_v2(resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    Fake node implementation of the v2 Push request protocol.

*******************************************************************************/

private scope class PushImpl_v2 : PushProtocol_v2
{
    import fakedmq.Storage;

    /***************************************************************************

        Constructor.

        Params:
            shared_resources = DMQ request resources getter

    ***************************************************************************/

    public this ( IRequestResources resources)
    {
        super(resources);
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

    override protected bool prepareChannels ( in cstring[] channel_names )
    {
        foreach ( channel; channel_names )
        {
            if ( global_storage.getCreate(channel) is null )
                return false;
        }

        return true;
    }

    /***************************************************************************

        Push a record to the specified storage channel.

        Params:
            channel_name = channel to push to
            value = record value to push

        Returns:
            true if the record was pushed to the channel, false if it failed

    ***************************************************************************/

    override protected bool pushToStorage ( cstring channel_name,
        in void[] value )
    {
        global_storage.getCreate(channel_name).push(value);

        return true;
    }
}
