/*******************************************************************************

    Checkers which uses the Consume request.

    There are two checkers, one which starts a Consume request before the writer
    runs and one which starts a Consume request after the writer has run. The
    DMQ node's behaviour is different in these two cases, so they are tested
    separately.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.cases.checkers.ConsumeChecker;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.core.Test;
import ocean.task.Task;
import ocean.meta.types.Qualifiers;
import dmqtest.cases.checkers.model.IChecker;
import dmqtest.DmqClient;

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

    private DmqClient.Consumer[const(char[])] consumers;

    /***************************************************************************

        DMQ client to use.

    ***************************************************************************/

    private DmqClient dmq;

    /***************************************************************************

        Constructor, sets the channels.

        Params:
            channels = list of channel names to consume from

    ***************************************************************************/

    protected this ( const(char[])[] channels )
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

    override public void check ( const(char[])[] records )
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
                assumeSafeAppend(consumer.data);
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
    do
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

    public this ( const(char[])[] test_channels )
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

    public this ( const(char[])[] test_channels )
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

    override public void check ( const(char[])[] records )
    {
        this.startConsumers();
        super.check(records);
    }
}


/*******************************************************************************

    Base for neo consume tests. Contains the required methods:
        1. To start a set of consumers (one per channel being checked)
        2. To verify the data received by the consumers.

*******************************************************************************/

private abstract class NeoConsumeCheckerBase : IChecker
{
    import dmqtest.util.RecordIndex;

    /***************************************************************************

        Set of active consumers.

    ***************************************************************************/

    private DmqClient.Neo.Consumers consumers;

    /***************************************************************************

        Channels being consumed from.

    ***************************************************************************/

    protected RecordIndex local;

    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = names of channels written to by the writer

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        this.local = new RecordIndex(test_channels);
    }

    /***************************************************************************

        Prepares for checking using `dmq`.
        Public use: Should be called before calling `this.check()`.
        Subclass use: Should be called before calling `this.startConsumers()`.

        Params:
            dmq = DMQ client to access the real DMQ

    ***************************************************************************/

    override public void prepare ( DmqClient dmq )
    {
        this.consumers = dmq.neo.new Consumers;
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`.

        Public use: Should be called after `this.prepare()` has returned.
        Subclass use: Should be called after `this.startConsumers()` has
        returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( const(char[])[] records )
    {
        size_t popped;
        auto expected_record_count = this.local.fill(records);

        while ( popped < expected_record_count )
        {
            // Wait for something to happen
            this.consumers.waitNextEvent();

            // Pop received records
            foreach ( record; this.consumers.received_records )
            {
                test!("==")(record.value, this.local.pop(record.channel));
                popped++;
            }
            this.consumers.received_records.length = 0;
            assumeSafeAppend(this.consumers.received_records);
        }

        test!("==")(popped, expected_record_count);

        this.consumers.stop();
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
        assert(this.consumers, "Call prepare() first");
    }
    do
    {
        foreach ( channel; this.local )
            this.consumers.startConsumer(channel);
    }
}


/*******************************************************************************

    Checker which initiates a Consume request before the writer has written any
    records to the DMQ.

*******************************************************************************/

class NeoPreConsumeChecker : NeoConsumeCheckerBase
{
    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Starts one consumer per specified channel.
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

class NeoPostConsumeChecker : NeoConsumeCheckerBase
{
    /***************************************************************************

        Constructor.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`.

        Should be called after `this.prepare()` has returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( const(char[])[] records )
    {
        this.startConsumers();
        super.check(records);
    }
}


/*******************************************************************************

    Base for neo consume tests where the request is suspended and resumed as it
    is processed. Contains the required methods:
        1. To start a set of consumers (one per channel being checked)
        2. To verify the data received by the consumers.

*******************************************************************************/

private abstract class NeoSuspendConsumeCheckerBase : NeoConsumeCheckerBase
{
    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = names of channels written to by the writer

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`. Suspends and resumes the request, periodically.

        Should be called after `this.prepare()` has returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( const(char[])[] records )
    {
        size_t popped;
        auto expected_record_count = this.local.fill(records);

        auto suspend_step = 100 * this.local.num_channels;
        ulong suspend_at = suspend_step;
        while ( popped < expected_record_count )
        {
            // Wait for something to happen
            this.consumers.waitNextEvent();

            // Pop received records
            foreach ( record; this.consumers.received_records )
            {
                test!("==")(record.value, this.local.pop(record.channel));
                popped++;
            }
            this.consumers.received_records.length = 0;
            assumeSafeAppend(this.consumers.received_records);

            if ( popped > suspend_at )
            {
                suspend_at += suspend_step;

                this.consumers.suspend();
                this.consumers.resume();
            }
        }

        test!("==")(popped, expected_record_count);

        this.consumers.stop();
    }
}

/*******************************************************************************

    Checker which initiates a Consume request before the writer has written any
    records to the DMQ. The request is suspended and resumed as it is processed.

*******************************************************************************/

class NeoSuspendPreConsumeChecker : NeoSuspendConsumeCheckerBase
{
    /***************************************************************************

        Constructor, sets the channels.

        Params:
            test_channels = test_channels to consume from

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Starts one consumer per specified channel.
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
    the DMQ. The request is suspended and resumed as it is processed.

*******************************************************************************/

class NeoSuspendPostConsumeChecker : NeoSuspendConsumeCheckerBase
{
    /***************************************************************************

        Constructor.

        Params:
            test_channels = channels to consume from

    ***************************************************************************/

    public this ( const(char[])[] test_channels )
    {
        super(test_channels);
    }

    /***************************************************************************

        Checks that for each channel the data received by the consumers matches
        `records`. Suspends and resumes the request, periodically.

        Should be called after `this.prepare()` has returned.

        Params:
            records = the records expected to receive from each channel

        Throws:
            TestException if the check fails

    ***************************************************************************/

    override public void check ( const(char[])[] records )
    {
        this.startConsumers();
        super.check(records);
    }
}
