/*******************************************************************************

    GetChannels request class.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.GetChannels;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.GetChannels;

/*******************************************************************************

    GetChannels request

*******************************************************************************/

public scope class GetChannels : Protocol.GetChannels
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();
}
