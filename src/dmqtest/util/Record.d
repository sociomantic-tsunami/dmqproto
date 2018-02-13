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

import ocean.text.convert.Formatter;
import core.stdc.stdlib;

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
    return format("{:d9}", i); // i.max has 9 decimal digits
}

/*******************************************************************************

    Sorts `array` in-place in ascending order according to `typeid(T).compare`.
    This function substitutes the deprecated array `.sort` property.

    Trials have shown that `ocean.core.array.Mutation.sort` causes a stack
    overflow when called with an array of `cstring` with 10,000 elements.

    Params:
        array = the array to sort

    Returns:
        sorted `array`.

*******************************************************************************/

T[] sort ( T ) ( T[] array )
{
    qsort(array.ptr, array.length, T.sizeof, &cmp!(T));
    return array;
}

/// `qsort` element comparison callback function, wraps `typeid(T).compare`.
extern (C) private int cmp ( T ) ( in void* a, in void* b )
{
    return typeid(T).compare(a, b);
}
