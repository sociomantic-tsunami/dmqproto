/*******************************************************************************

    Abstract base class that acts as a root for all DMQ protocol classes

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.request.model.DmqCommand;

import ocean.transition;

import swarm.node.protocol.Command;
import swarm.protocol.StringListReader;
import dmqproto.client.legacy.DmqConst;

public abstract scope class DmqCommand : Command
{
    /***************************************************************************
    
        Holds set of method to access temporary resources used by dmqnode
        protocol classes. Those all are placed into single class to simplify
        maintenance and eventually may be replaced with more automatic approach.

    ***************************************************************************/

    public interface Resources
    {
        mstring*          getChannelBuffer ( );
        mstring*          getValueBuffer ( );
        StringListReader getChannelListReader ( );
    }

    /***************************************************************************

        Specific implementation of shared resource getters. Passed through
        constructors from acual protocol implementation which can make
        decision about how resources need to be handled.
        
    ***************************************************************************/

    protected Resources resources;

    /***************************************************************************

        Constructor

        Params:
            command = command code
            reader = FiberSelectReader instance to use for read requests
            writer = FiberSelectWriter instance to use for write requests

    ***************************************************************************/

    public this ( DmqConst.Command.E command, FiberSelectReader reader,
        FiberSelectWriter writer, Resources resources )
    {
        auto pname = command in DmqConst.Command();
        auto name  = pname ? *pname : "?";
        super(name, reader, writer);
        this.resources = resources;
    }
}
