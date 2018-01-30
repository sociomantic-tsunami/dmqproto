/*******************************************************************************

    Fake DMQ node Pop request implementation.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.request.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.neo.request.core.IRequestResources;
import dmqproto.node.neo.request.Pop;

import fakedmq.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;
import dmqproto.client.legacy.DmqConst;

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
        case 0:
            scope rq = new PopImpl_v0(resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addCopy(SupportedStatus.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    Fake node implementation of the v0 Pop request protocol.

*******************************************************************************/

private scope class PopImpl_v0 : PopProtocol_v0
{
    import fakedmq.Storage;

    /***************************************************************************

        Remember consumed queue

    ***************************************************************************/

    private Queue queue;

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

        Performs any logic needed to pop from the channel of the given name.

        Params:
            channel_name = channel to pop from
            subscribed = `true` if the return value is `false` because the
                channel has subscribers so it is not possible to pop from it

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name,
        out bool subscribed )
    {
        this.queue = global_storage.getCreate(channel_name).queue_unless_subscribed;
        subscribed = this.queue is null;
        return !subscribed;
    }

    /***************************************************************************

        Pop the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextValue ( ref void[] value )
    {
        size_t records, bytes;
        this.queue.countSize(records, bytes);

        if ( records == 0 )
            return false;

        value = this.queue.pop().dup;
        return true;
    }
}
