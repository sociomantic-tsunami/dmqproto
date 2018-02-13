/*******************************************************************************

    DMQ neo request codes.

    Note that the neo DMQ protocol used the legacy request codes defined in
    dmqproto.client.legacy.DmqConst, in the beginning. The codes defined in this module thus
    mirror the legacy codes.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.common.RequestCodes;

public enum RequestCode : ubyte
{
    None,
    Push,
    Pop,
    Consume = 6
}
