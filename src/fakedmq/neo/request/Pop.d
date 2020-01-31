/*******************************************************************************

    Fake DMQ node Pop request implementation.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.request.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.neo.request.Pop;

import ocean.meta.types.Qualifiers;

/*******************************************************************************

    Fake node implementation of the v1 Pop request protocol.

*******************************************************************************/

class PopImpl_v1 : PopProtocol_v1
{
    import fakedmq.Storage;
    import dmqproto.node.neo.request.core.IRequestResources;

    /***************************************************************************

        Remember consumed queue

    ***************************************************************************/

    private Queue queue;

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

    override protected bool prepareChannel ( IRequestResources resources,
        cstring channel_name, out bool subscribed )
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
