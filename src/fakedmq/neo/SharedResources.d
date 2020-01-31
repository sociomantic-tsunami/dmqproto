/*******************************************************************************

    Fake DMQ node neo request shared resources getter.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.SharedResources;

import ocean.meta.types.Qualifiers;

import dmqproto.node.neo.request.core.IRequestResources;

/*******************************************************************************

    Provides resources required by the protocol. As this implementation is fpr
    testing purposes only, it simply allocates as much stuff as necessary to
    keep the code simple.

*******************************************************************************/

class SharedResources : IRequestResources
{
    /***************************************************************************

        Struct wrapper used to workaround D's inability to allocate slices on
        the heap via `new`.

    ***************************************************************************/

    private static struct Buffer
    {
        void[] data;
    }

    /***************************************************************************

        Returns:
            a new buffer to store record values in

    ***************************************************************************/

    override public void[]* getVoidBuffer ( )
    {
        return &((new Buffer).data);
    }
}
