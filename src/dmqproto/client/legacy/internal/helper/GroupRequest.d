/*******************************************************************************

    Group request manager alias template.

    Usage example:

    Copyright:
        Copyright (c) 2012-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.internal.helper.GroupRequest;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.client.helper.GroupRequest;

import dmqproto.client.DmqClient;

import dmqproto.client.legacy.internal.request.notifier.RequestNotification;

import dmqproto.client.legacy.internal.request.params.RequestParams;



/*******************************************************************************

    Group request manager alias template.

    Template params:
        Request = type of request struct to manage (should be one of the structs
            returned by the DMQ client request methods)

*******************************************************************************/

public template GroupRequest ( Request )
{
    alias IGroupRequestTemplate!(Request, RequestParams, RequestNotification)
        GroupRequest;
}

