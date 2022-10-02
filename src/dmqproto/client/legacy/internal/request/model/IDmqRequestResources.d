/*******************************************************************************

    Interface and base class containing getter methods to acquire
    resources needed by a DMQ client request. Multiple calls to the same
	getter only result in the acquiring of a single resource of that type, so
	that the same resource is used over the life time of a request. When a
	request resource instance goes out of scope all required resources are
	automatically relinquished.

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.request.model.IDmqRequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.common.request.model.IRequestResources;

import dmqproto.client.legacy.internal.connection.SharedResources;

import swarm.client.request.model.IFlushable : IFlushables;

import swarm.client.connection.model.INodeConnectionPoolInfo;

import dmqproto.client.legacy.internal.connection.model.IReregistrator;

import swarm.client.ClientExceptions :
    EmptyValueException, FatalErrorException;

import dmqproto.client.legacy.internal.DmqClientExceptions : AllNodesFailedException;



/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (dmqproto.client.legacy.internal.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding some additional
    DMQ-specific getters.

*******************************************************************************/

public interface IDmqRequestResources : IRequestResources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .LoopCeder LoopCeder;
    alias .ValueProducer ValueProducer;
    alias .RequestSuspender RequestSuspender;
    alias .IReregistrator IReregistrator;
    alias .AllNodesFailedException AllNodesFailedException;
    alias .FatalErrorException FatalErrorException;
    alias .EmptyValueException EmptyValueException;


    /***************************************************************************

        Flushables set getter.

    ***************************************************************************/

    IFlushables flushables ( );


    /***************************************************************************

        Connection pool info interface getter.

    ***************************************************************************/

    INodeConnectionPoolInfo conn_pool_info  ( );

    /***************************************************************************

        Connection reregistrator getter.

    ***************************************************************************/

    IReregistrator reregistrator ( );


    /***************************************************************************

        All nodes failed exception getter.

    ***************************************************************************/

    AllNodesFailedException all_nodes_failed_exception ( );


    /***************************************************************************

        Fatal exception getter.

    ***************************************************************************/

    FatalErrorException fatal_error_exception ( );


    /***************************************************************************

        Empty value exception getter.

    ***************************************************************************/

    EmptyValueException empty_value_exception ( );
}



/*******************************************************************************

    Mix in a class called RequestResources which implements
    IRequestResources.

    Note that this class does not implement the additional methods required by
    IDmqRequestResources -- this is done in
    dmqproto.client.legacy.internal.connection.DmqRequestConnection.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);

