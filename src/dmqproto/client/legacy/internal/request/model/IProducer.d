/*******************************************************************************

    Interface to a process which can be provided with values to send.

    Copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.model.IProducer;

import swarm.client.request.model.INodeInfo;
import swarm.client.request.model.IContextInfo;

import ocean.meta.types.Qualifiers;


public interface IProducer : IContextInfo, INodeInfo
{
    /***************************************************************************

        Provides a value to be sent. Sending an empty string causes the produce
        stream to end.

        This method is aliased as opCall.

        Params:
            value = value to send to node

        Returns:
            true if the value is accepted to be sent, false if the sending
            process is in the middle of sending a previous value

    ***************************************************************************/

    public bool produce ( cstring value );

    public alias produce opCall;
}

