/*******************************************************************************

    ProduceMulti request class.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.ProduceMulti;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import Protocol = dmqproto.node.request.ProduceMulti;

/*******************************************************************************

    ProduceMulti request

*******************************************************************************/

public class ProduceMulti : Protocol.ProduceMulti
{
    import fakedmq.Storage;
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Pushes a received record to one or more queues. To be overriden by
        an actual implementors of queuenode protocol.

        Params:
            channel_names = names of channels to push to
            value = record value to push

    ***************************************************************************/

    override protected void pushRecord ( in cstring[] channel_names, cstring value )
    {
        foreach (channel; channel_names)
        {
            global_storage.getCreate(channel).push(value);
        }
    }
}
