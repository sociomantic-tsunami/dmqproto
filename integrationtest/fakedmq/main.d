/*******************************************************************************

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.fakedmq.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import turtle.runner.Runner;
import dmqtest.TestRunner;

/*******************************************************************************

     Entry point

*******************************************************************************/

version ( unittest ) {} else
int main ( istring[] args )
{
    auto runner = new TurtleRunner!(DmqTestRunner)("fakedmq", "dmqtest.cases");
    return runner.main(args);
}
