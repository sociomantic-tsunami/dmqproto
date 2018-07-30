/*******************************************************************************

    Automatically generated tests for all combinations of writers x checkers.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

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

    alias Tuple!(PushWriter, PushMultiWriter, ProduceWriter, ProduceMultiWriter,
        NeoPushWriter, NeoPushMultiWriter) Writers;
    alias Tuple!(PopChecker, PreConsumeChecker, PostConsumeChecker,
        NeoPopChecker, NeoPreConsumeChecker, NeoPostConsumeChecker) Checkers;

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

    Test for all Neo writers with all Neo checkers, running writer and checker
    in parallel.

*******************************************************************************/

class ParallelWriterChecker: MultiTestCase
{
    /***************************************************************************

        Tuples defining the set of writers and checkers to combine to generate
        the test cases where writer and checker should be run in parallel.

    ***************************************************************************/

    alias Tuple!(NeoPushWriter, NeoPushMultiWriter) Writers;
    alias Tuple!(ParallelNeoPopChecker, NeoPreConsumeChecker,
        NeoPostConsumeChecker, NeoSuspendPreConsumeChecker,
        NeoSuspendPostConsumeChecker) Checkers;

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
                this.test_cases[i++] =
                    newTestCase!(ParallelCheckedDmqTestCase, W, C);
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

    Tests for channel subscriptions with all writers

*******************************************************************************/

class WriterSubscriber: MultiTestCase
{
    /***************************************************************************

        Tuples defining the set of writers and checkers to combine to generate
        the test cases.
        These are the WriterChecker writers except Produce. With a Produce
        request, unlike the other types of writer, the test cannotknow when the
        produced records actually arrive in the storage engine. Depending on the
        implementation of the storage engine, it may be possible (due to race
        conditions) for the checker request to start being handled before all
        produced records have been added to the storage. This can lead to test
        failures.

    ***************************************************************************/

    alias Tuple!(PushWriter, PushMultiWriter, NeoPushWriter, NeoPushMultiWriter)
        Writers;

    /***************************************************************************

        Array of test cases.

    ***************************************************************************/

    private TestCase[Writers.length] test_cases;

    /***************************************************************************

        Constructor, creates all test cases.

    ***************************************************************************/

    public this ( )
    {
        uint i = 0;
        foreach ( W; Writers )
            this.test_cases[i++] = new SubscribeDmqTestCase(new W);
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
