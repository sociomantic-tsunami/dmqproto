/*******************************************************************************

    Helper function to generate ordered record values.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.util.Record;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.text.convert.Format;

import ocean.transition;


/*******************************************************************************

    Generates a record value deterministically from the provided integer.

    Params:
        i = integer to generate record from

    Returns:
        record value corresponding to i

*******************************************************************************/

istring getRecord ( uint i )
{
    mstring ret;
    Format.format(ret, "{}", i);
    return assumeUnique(ret);
}

