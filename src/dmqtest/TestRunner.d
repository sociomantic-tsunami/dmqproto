/*******************************************************************************

    Reusable test runner class for testing any DMQ node implementation, based
    on turtle facilities. In most cases implies providing DMQ node binary name
    to runner constructor should be enough.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.TestRunner;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import ocean.util.log.Logger;

import turtle.runner.Runner;

import dmqtest.cases.Basic;
import dmqtest.cases.Consume;
import dmqtest.cases.WriterChecker;


/*******************************************************************************

    Test runner specialized for DMQ nodes

*******************************************************************************/

class DmqTestRunner : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    public this()
    {
        this.test_package = "dmqtest.cases";
    }

    /***************************************************************************

        Filter out console output spam from tested application

    ***************************************************************************/

    override public void prepare ( )
    {
        auto app_log = Log.lookup(this.config.name);
        app_log.level(Level.Warn);
    }

    /***************************************************************************

        No arguments but add small startup delay to let DMQ node initialize
        listening socket.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out string[] args, out string[string] env )
    {
        delay = 1.0;
        args  = null;
        env   = null;
    }
}
