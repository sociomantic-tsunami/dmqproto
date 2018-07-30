/*******************************************************************************

    GetChannelSize request class.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.GetChannelSize;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.GetChannelSize;

/*******************************************************************************

    GetChannelSize request

*******************************************************************************/

public scope class GetChannelSize : Protocol.GetChannelSize
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();
}

