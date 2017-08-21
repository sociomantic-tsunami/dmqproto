/*******************************************************************************

    PushMulti request class.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.PushMulti;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.PushMulti;

/*******************************************************************************

    PushMulti request

*******************************************************************************/

public scope class PushMulti : Protocol.PushMulti
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        No-op, channels are created lazily in this.pushValue

    ***************************************************************************/

    override protected bool prepareChannels ( in cstring[] channel_names )
    {
        return true;
    }

    /***************************************************************************

        Push the value to the channel.

        Params:
            channel_name = name of channel to be writter to
            value        = value to write

        Returns:
            "true" if writing the value was possible
            "false" if there wasn't enough space

    ***************************************************************************/

    override protected void pushValue ( cstring channel_name, in void[] value )
    {
        global_storage.getCreate(channel_name).push(value);
    }
}
