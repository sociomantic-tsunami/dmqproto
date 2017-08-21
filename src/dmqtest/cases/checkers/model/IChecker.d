/*******************************************************************************

    Base class for checkers, to test the results of a writer.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.checkers.model.IChecker;

abstract class IChecker
{
    import ocean.core.Test; // makes `test` available in derivatives
    import dmqtest.DmqClient;

    import ocean.transition;

    /***************************************************************************

        Prepares for checking using `dmq`.
        Should be called before calling `this.check()`.

        Params:
            DMQ = DMQ client to use

    ***************************************************************************/

    abstract public void prepare ( DmqClient dmq );

    /***************************************************************************

        Performs the checking part of the test case, comparing the data provided
        in `records` against the DMQ and throwing on error.

        Should be called after `this.prepare()` has returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    abstract public void check ( Const!(char[])[] records );
}

