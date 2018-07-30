/*******************************************************************************

    Produce request class.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.Produce;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.Produce;

/*******************************************************************************

    Produce request

*******************************************************************************/

public scope class Produce : Protocol.Produce
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Pushes a received record to the queue.

        Params:
            channel_name = name of channel to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( cstring channel_name, cstring value )
    {
        global_storage.getCreate(channel_name).push(value);
    }
}
