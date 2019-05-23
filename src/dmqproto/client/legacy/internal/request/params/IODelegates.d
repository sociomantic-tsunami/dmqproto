/*******************************************************************************

    I/O delegates of DMQ client requests, providing feedback between the DMQ
    client and the user.

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.params.IODelegates;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.client.request.context.RequestContext;

import swarm.client.request.model.ISuspendableRequest;
import swarm.client.request.model.IStreamInfo;

import dmqproto.client.legacy.internal.request.model.IProducer;


/*******************************************************************************

    Aliases for the IO delegates.

    N.B. it is important that the context is
    passed to all the delegates, as the users heavily rely on the request
    context inside the delegates.

*******************************************************************************/


/*******************************************************************************

   Alias for delegate which puts single values

*******************************************************************************/

public alias Const!(char[]) delegate (RequestContext) PutValueDg;


/*******************************************************************************

    Alias for delegate which gets single values

*******************************************************************************/

public alias void delegate (RequestContext, Const!(char[])) GetValueDg;


/*******************************************************************************

    Alias for delegate which gets a node's value (used to get the API
    version number and channel list).

*******************************************************************************/

public alias void delegate (RequestContext, Const!(char[]), ushort, Const!(char[])) GetNodeValueDg;


/*******************************************************************************

    Alias for delegate which gets a node's number of active connections
    (as returned by GetNumConnections)

*******************************************************************************/

public alias void delegate (RequestContext, Const!(char[]), ushort, size_t) GetNumConnectionsDg;


/*******************************************************************************

    Alias for delegate which gets an ISuspendable interface for a request

*******************************************************************************/

public alias void delegate (RequestContext, ISuspendableRequest) RegisterSuspendableDg;


/*******************************************************************************

    Alias for delegate which gets an IProducer interface for a request

*******************************************************************************/

public alias void delegate (RequestContext, IProducer) ProducerDg;


/*******************************************************************************

    Alias for delegate which gets an IStreamInfo interface for a request

*******************************************************************************/

public alias void delegate (RequestContext, IStreamInfo) RegisterStreamInfoDg;
