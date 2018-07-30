/*******************************************************************************

    Fake DMQ node Push request implementation.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.request.Push;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.neo.request.Push;

import ocean.transition;

/*******************************************************************************

    Fake node implementation of the v2 Push request protocol.

*******************************************************************************/

class PushImpl_v3 : PushProtocol_v3
{
    import fakedmq.Storage;
    import dmqproto.node.neo.request.core.IRequestResources;

    /***************************************************************************

        Ensures that requested channels exist / can be created and can be
        written to.

        Params:
            resources = request resources
            channel_names = list of channel names to check

        Returns:
            "true" if all requested channels are available
            "false" otherwise

    ***************************************************************************/

    override protected bool prepareChannels ( IRequestResources resources,
        in cstring[] channel_names )
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
            resources = request resources
                channel_name = channel to push to
            value = record value to push

        Returns:
            true if the record was pushed to the channel, false if it failed

    ***************************************************************************/

    override protected bool pushToStorage ( IRequestResources resources,
        cstring channel_name, in void[] value )
    {
        global_storage.getCreate(channel_name).push(value);

        return true;
    }
}
