/*******************************************************************************

    Push request class.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.Push;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import Protocol = dmqproto.node.request.Push;

/*******************************************************************************

    Push request

*******************************************************************************/

public class Push : Protocol.Push
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Push the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

    ***************************************************************************/

    override protected void pushValue ( cstring channel_name, in void[] value )
    {
        global_storage.getCreate(channel_name).push(value);
    }
}
