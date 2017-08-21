/*******************************************************************************

    GetNumConnections request class.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.GetNumConnections;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dmqproto.node.request.GetNumConnections;

/*******************************************************************************

    GetNumConnections request

*******************************************************************************/

public scope class GetNumConnections : Protocol.GetNumConnections
{
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();
}
