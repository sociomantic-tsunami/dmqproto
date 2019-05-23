/*******************************************************************************

    DMQ Client & Node Constants

    Copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.legacy.DmqConst;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.Const;

import ocean.core.Enum;


import ocean.core.Tuple;



/*******************************************************************************

    DmqConst

*******************************************************************************/

public struct DmqConst
{
static:

    /***************************************************************************

        Command Code definitions

        Push                = push a record to the DMQ
        Pop                 = pop a record from the DMQ
        Consume             = initiates an endless loop of receiving records
                              from the DMQ as they are pushed into it
        GetNumConnections   = gets the current number of active connections
                              from a DMQ node
        PushMulti           = pushes a record to multiple DMQ channels
        Produce             = pushes a stream of records to a DMQ channel
        ProduceMulti        = pushes a stream of records to multiple  DMQ
                              channels
        RemoveChannel       = completely deletes a DMQ channel

    ***************************************************************************/

    public class Command : ICommandCodes
    {
        mixin EnumBase!([
            "Push"[]:                  1,
            "Pop":                     2,
            //"GetChannels":             3,
            //"GetChannelSize":          4,
            //"GetSize":                 5,
            "Consume":                 6,
            //"GetSizeLimit":            7,
            "GetNumConnections":       8,
            "PushMulti":               9,
            "Produce":                 10,
            "ProduceMulti":            11,
            "RemoveChannel":           12
            //"PushMulti2":              13
        ]);
    }


    /***************************************************************************

        Status Code definitions (sent from the node to the client)

        Code 0   = Uninitialised value, never returned by the node.
        Code 200 = Node returns OK when request was fulfilled correctly.
        Code 400 = Node throws this error when the received command is not
                   recognized.
        Code 406 = The node does not support this command.
        Code 407 = Out of memory error in node (DMQ channel full).
        Code 408 = Attempted to push an empty value (which is illegal).
        Code 409 = Request channel name is invalid.
        Code 500 = This error indicates an internal node error.

    ***************************************************************************/

    public alias IStatusCodes Status;


    /***************************************************************************

        Node Item

    ***************************************************************************/

    public alias .NodeItem NodeItem;
}

