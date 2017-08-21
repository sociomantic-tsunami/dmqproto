/******************************************************************************

    IDmqNodeRegistry defines public / external methods on a DMQ client's
    node registry. Instances of this interface can be safely exposed externally
    to the DMQ client.

    Copyright:
        Copyright (c) 2010-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.registry.model.IDmqNodeRegistry;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.registry.model.INodeRegistryInfo;



/*******************************************************************************

    DMQ connection registry interface

*******************************************************************************/

public interface IDmqNodeRegistry : INodeRegistryInfo
{
}

