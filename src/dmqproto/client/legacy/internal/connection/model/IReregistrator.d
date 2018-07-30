/*******************************************************************************

    An IReregistrator instance is used in certain IRequest subclasses to
    reassign a request to the next node if the current node failed.

    Copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.connection.model.IReregistrator;



/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.client.legacy.internal.request.params.RequestParams;



/*******************************************************************************

    Enum that defines the possible return values for the registerNext method.
    Either the query was successfully reregistered, all nodes have already been
    tried, or the query is a multiple node query and should not be
    re-registered.

*******************************************************************************/

public enum RegisterNextResult
{
    Reregistered        = 0,
    NoMoreNodes         = 1,
    MultipleNodeQuery   = 2
}



/*******************************************************************************

    Request reregistrator interface, used by requests which retry sequentially
    over all nodes until the request succeeds or all nodes have been tried.

*******************************************************************************/

interface IReregistrator
{
    /***************************************************************************

        Adds params to the next DMQ node according to the node ID counter of
        params, if there are nodes which have not been queried by previous
        calls with this particular request and the request is a single node
        request (push, pop, pushMulti).

        Params:
            params = request parameters

        Returns:
            Reregistered if params has been assigned to the next node or
            NoMoreNodes if, according to the node ID counter of request_item,
            all nodes have been queried by previous calls with this particular
            request, or MultipleNodeQuery if this request is a multiple node
            request and should not be re-registered.

    ***************************************************************************/

    RegisterNextResult registerNext ( RequestParams params );
}

