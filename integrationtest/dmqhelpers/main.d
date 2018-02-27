/*******************************************************************************

    Verifies behaviour of turtle.env.Dmq helpers that expect running tested
    application

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.dmqhelpers.main;

import ocean.transition;
import ocean.task.Scheduler;

import turtle.runner.Runner;
import turtle.TestCase;
import turtle.env.Dmq;

version (UnitTest) {} else
int main ( istring[] args )
{
    return (new TurtleRunner!(MyTurtleTests)("dmqapp")).main(args);
}

class MyTurtleTests : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    override protected void configureTestedApplication ( out double delay,
        out istring[] args, out istring[istring] env )
    {
        delay = 1.0;
        args  = null;
        env   = null;
    }

    override public void prepare ( )
    {
        Dmq.initialize();
        dmq.start("127.0.0.1", 0);
        dmq.genConfigFiles(this.context.paths.sandbox ~ "/etc");
    }

    override public void reset ( )
    {
        dmq.clear();
    }
}

class WaitTotalRecords : TestCase
{
    import ocean.core.Test;

    override public Description description ( )
    {
        return Description("dmq.waitTotalRecords: normal case");
    }

    override public void run ( )
    {
        dmq.push("test_channel1", "record");
        dmq.waitTotalRecords("test_channel2", 1);
        test!("==")(dmq.pop("test_channel2"), "record");
    }
}
