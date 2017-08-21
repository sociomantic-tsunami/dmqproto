/*******************************************************************************

    RemoveChannel request.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.RemoveChannel;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.RemoveChannel;

/*******************************************************************************

    RemoveChannel request

*******************************************************************************/

public scope class RemoveChannel : Protocol.RemoveChannel
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Removes the specified channel from the storage engine

    ***************************************************************************/

    override protected void removeChannel ( cstring channel )
    {
        global_storage.remove(channel);
    }
}
