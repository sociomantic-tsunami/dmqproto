/*******************************************************************************

    Test DMQ node restart functionality in combination with external DMQ client
    doing `Consume` request. Uses `dmqapp` as a tested application which will
    start consuming "test_channel1" and push all records to "test_channel2".

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module integrationtest.dmqrestart.main;

import ocean.meta.types.Qualifiers;

import turtle.runner.Runner;
import turtle.TestCase;

/// ditto
class DmqRestartTests : TurtleRunnerTask!(TestedAppKind.Daemon)
{
    import turtle.env.Dmq;

    override protected void configureTestedApplication ( out double delay,
        out istring[] args, out istring[istring] env )
    {
        delay = 0.1;
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

version ( unittest ) { }
else
int main ( istring[] args )
{
    auto runner = new TurtleRunner!(DmqRestartTests)("dmqapp", "");
    return runner.main(args);
}

/*******************************************************************************

    Verifies scenario where test cases pushes records to a channel tested app
    listens on, both before and after fake node restart.

*******************************************************************************/

class RestartWithConsumer : TestCase
{
    import turtle.env.Dmq;

    import ocean.core.Test;
    import ocean.task.util.Timer;

    override void run ( )
    {
        dmq.push("test_channel1", "value");
        wait(100_000); // small delay to ensure fakedmq manages to process
                       // `Push` request to "test_channel2"

        dmq.stop();
        dmq.restart();
        wait(300_000); // small delay to ensure tested app reassigns Listen

        dmq.push("test_channel1", "value2");
        wait(100_000); // small delay to ensure fakedmq manages to process
                       // `Push` request to "test_channel2"
        test!("==")(dmq.pop!(cstring)("test_channel2"), "value");
        test!("==")(dmq.pop!(cstring)("test_channel2"), "value2");
    }
}
