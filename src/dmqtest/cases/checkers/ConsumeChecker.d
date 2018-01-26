/*******************************************************************************

    Checkers which uses the Consume request.

    There are two checkers, one which starts a Consume request before the writer
    runs and one which starts a Consume request after the writer has run. The
    DMQ node's behaviour is different in these two cases, so they are tested
    separately.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.checkers.ConsumeChecker;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.task.Task;
import ocean.transition;
import dmqtest.cases.checkers.model.IChecker;

/*******************************************************************************

    Base for consume tests. Contains the required methods:
        1. To start a set of consumers (one per channel being checked)
        2. To verify the data received by the consumers.

*******************************************************************************/

private abstract class ConsumeCheckerBase : IChecker
{
    /***************************************************************************

        Set of active consumers per channel name.

    ***************************************************************************/

    private DmqClient.Consumer[Const!(char[])] consumers;

    /***************************************************************************

        DMQ client to use.

    ***************************************************************************/

    private DmqClient dmq;

    /***************************************************************************

        Constructor, sets the channels.

        Params:
            channels = list of channel names to consume from

    ***************************************************************************/

    protected this ( Const!(char[])[] channels )
    {
        foreach (channel; channels)
            this.consumers[channel] = null;
    }

    /***************************************************************************

        Prepares for consuming using `dmq`.
        Public use: Should be called before calling `this.check()`.
        Subclass use: Should be called before calling `this.startConsumers()`.

        Params:
            dmq = DMQ client to access the real DMQ

    ***************************************************************************/

    override public void prepare ( DmqClient dmq )
    {
        this.dmq = dmq;
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`.

        Public use: Should be called after `this.check()` has returned.
        Subclass use: Should be called after `this.startConsumers()` has
        returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( Const!(char[])[] records )
    {
        foreach ( channel, consumer; this.consumers )
        {
            assert(consumer);
            uint popped;
            while ( popped < records.length )
            {
                // Note: in error situations (if the node doesn't send any
                // records to the consumer), it is possible that the test will
                // just hang here. At the moment, there's no nice way to handle
                // this case and we just rely on CI to time out the stalled
                // test.
                consumer.waitNextEvent();

                foreach (consumed; consumer.data)
                {
                    test!("==")(consumed, records[popped++]);
                }

                consumer.data.length = 0;
                enableStomping(consumer.data);
            }

            test!("==")(consumer.data.length, 0);
            test!("==")(popped, records.length);
        }
    }

    /***************************************************************************

        Starts one consumer per specified test channel.
        Should be called
          - after `this.prepare()` has returned and
          - before calling `this.check()`.

    ***************************************************************************/

    protected void startConsumers ( )
    in
    {
        assert(this.dmq, "Call prepare() first");
    }
    body
    {
        foreach ( channel, ref consumer; this.consumers )
            consumer = this.dmq.startConsume(channel);
    }
}

/*******************************************************************************

    Checker which initiates a Consume request before the writer has written any
    records to the DMQ.

*******************************************************************************/

class PreConsumeChecker : ConsumeCheckerBase
{
    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( Const!(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Prepares for consuming using `dmq`.
        Should be called before calling `this.check()`.

        Params:
            dmq = DMQ client to access the real DMQ

    ***************************************************************************/

    override public void prepare ( DmqClient dmq )
    {
        super.prepare(dmq);
        this.startConsumers();
    }
}

/*******************************************************************************

    Checker which initiates a Consume request after the writer has written to
    the DMQ.

*******************************************************************************/

class PostConsumeChecker : ConsumeCheckerBase
{
    /***************************************************************************

        Constructor.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( Const!(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`.

        Should be called after `this.check()` has returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( Const!(char[])[] records )
    {
        this.startConsumers();
        super.check(records);
    }
}

