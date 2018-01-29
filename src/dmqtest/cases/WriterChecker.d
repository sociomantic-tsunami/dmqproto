/*******************************************************************************

    Automatically generated tests for all combinations of writers x checkers.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.WriterChecker;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.DmqTestCase;

import dmqtest.cases.writers.model.IWriter;
import dmqtest.cases.writers.PushWriter;
import dmqtest.cases.writers.PushMultiWriter;
import dmqtest.cases.writers.ProduceWriter;
import dmqtest.cases.writers.ProduceMultiWriter;

import dmqtest.cases.checkers.model.IChecker;
import dmqtest.cases.checkers.PopChecker;
import dmqtest.cases.checkers.ConsumeChecker;

import turtle.TestCase;

import ocean.core.Tuple : Tuple;

/*******************************************************************************

    Test for all writers with all checkers.

*******************************************************************************/

class WriterChecker: MultiTestCase
{
    /***************************************************************************

        Tuples defining the set of writers and checkers to combine to generate
        the test cases.

    ***************************************************************************/

    alias Tuple!(PushWriter, PushMultiWriter, ProduceWriter, ProduceMultiWriter)
        Writers;
    alias Tuple!(PopChecker, PreConsumeChecker, PostConsumeChecker)
        Checkers;

    /***************************************************************************

        Array of test cases.

    ***************************************************************************/

    private TestCase[Writers.length * Checkers.length] test_cases;

    /***************************************************************************

        Constructor, creates all test cases.

    ***************************************************************************/

    public this ( )
    {
        uint i = 0;
        foreach ( W; Writers )
        {
            foreach ( C; Checkers )
            {
                this.test_cases[i++] = newTestCase!(CheckedDmqTestCase, W, C);
            }
        }
        assert(i == this.test_cases.length);
    }

    /***************************************************************************

        Returns:
            the array of test cases.

    ***************************************************************************/

    override public TestCase[] getNestedCases ( )
    {
        return this.test_cases;
    }
}

/*******************************************************************************

    Creates a new test case object.

    Params:
        Test = test case base class
        W    = IWriter-derived class which is used to write data to the DMQ
        C    = IChecker-derived class which is used to check the data which the
            writer wrote to the DMQ

    Returns:
        a new test case object.

*******************************************************************************/

private Test newTestCase ( Test: DmqTestCase, W: IWriter, C: IChecker ) ( )
{
    auto writer = new W;
    return new Test(writer, new C(writer.test_channels));
}
