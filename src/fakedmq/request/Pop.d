/*******************************************************************************

    Pop request class.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import Protocol = dmqproto.node.request.Pop;

/*******************************************************************************

    Pop request

*******************************************************************************/

public scope class Pop : Protocol.Pop
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Pops the last value from the channel.

        Params:
            channel_name = name of channel to be queried

        Returns:
            popped value, empty array if channel is empty

    ***************************************************************************/

    override protected const(void)[] getNextValue ( cstring channel_name )
    {
        if (auto channel = global_storage.get(channel_name))
        {
            if (auto queue = channel.queue_unless_subscribed)
            {
                try
                    return queue.pop();
                catch (EmptyChannelException)
                { }
            }
        }

        return null;
    }
}
