/*******************************************************************************

    RemoveChannel request.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.RemoveChannel;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import Protocol = dmqproto.node.request.RemoveChannel;

/*******************************************************************************

    RemoveChannel request

*******************************************************************************/

public class RemoveChannel : Protocol.RemoveChannel
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
