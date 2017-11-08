/*******************************************************************************
    
    Common bases for all dmqtest test cases. Provides DMQ client instance and
    defines standard name for tested channel.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.DmqTestCase;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqtest.cases.writers.model.IWriter;
import dmqtest.cases.checkers.model.IChecker;

import turtle.TestCase;

import ocean.transition;

import ocean.task.Task;
import ocean.task.Scheduler;

/*******************************************************************************

    Basic test case base. Actual tests are located in `dmqtest.cases`.

*******************************************************************************/

abstract class DmqTestCase : TestCase
{
    import ocean.core.Test; // makes `test` available in derivatives
    import dmqtest.DmqClient;
    import dmqtest.util.Record;

    /***************************************************************************

        Number of records handled in bulk tests. (This value is used by all test
        cases which test reading/writing a large number of records from the DMQ.
        Small sanity check test cases do not use it.)

    ***************************************************************************/

    public const size_t bulk_test_record_count = 10_000;

    /***************************************************************************

        The records used in bulk test.

    ***************************************************************************/

    version (D_Version2)
        mixin(global("public immutable(char[][]) records;"));
    else
        public static char[][] records;

    /***************************************************************************

        DMQ client to use in tests. Provides blocking task API.

    ***************************************************************************/

    protected DmqClient dmq;

    /***************************************************************************

        The names of the channels with test data which will be cleaned
        automatically after the test case ends.

    ***************************************************************************/

    protected Const!(char[])[] channels;

    /***************************************************************************

        Constructor

        Params:
            channels = the names of the used DMQ channels, removed on cleanup;
                if empty, the standard name for a channel with test data is used

    ***************************************************************************/

    protected this ( Const!(char[])[] channels = null )
    {
        if (channels.length)
        {
            this.channels = channels;
        }
        else
        {
            mixin(global("static istring default_channel = \"test_channel\""));
            this.channels = (&default_channel)[0 .. 1];
        }
    }

    /***************************************************************************

        Initializes internal DMQ client

    ***************************************************************************/

    override public void prepare ( )
    {
        this.dmq = new DmqClient();
        this.dmq.addNode("127.0.0.1", 10000);
        this.dmq.neo.connect();
    }

    /***************************************************************************

        Delete the test channels each time the test case finishes, to avoid
        using some state by accident between tests.

    ***************************************************************************/

    override public void cleanup ( )
    {
        foreach ( channel; this.channels )
        {
            this.dmq.removeChannel(channel);
        }

        this.dmq.neo.shutdown();
    }

    /***************************************************************************

        Create the test records.

    ***************************************************************************/

    const static_this =
    `
        static this ( )
        {
            auto records = new istring[bulk_test_record_count];
            foreach (uint i, ref record; records)
                record = getRecord(i);
            this.records = assumeUnique(records);
        }
    `;

    version (D_Version2)
        mixin("shared" ~ static_this);
    else
        mixin(static_this);

}

/*******************************************************************************

    Class for test cases which work via a writer and a checker.

*******************************************************************************/

class CheckedDmqTestCase : DmqTestCase
{
    import ocean.text.util.ClassName;

    /***************************************************************************

        The writer to use.

    ***************************************************************************/

    protected IWriter writer;

    /***************************************************************************

        The checker to use.

    ***************************************************************************/

    protected IChecker checker;

    /***************************************************************************

        Constructor, creates instances of the Writer and Checker.

        Note that these classes are assumed to have constructors of a specific
        form, as follows.

        Writer's constructor is expected to accept no arguments.

        Expected form of Checker's constructor:
            this ( Const!(char[])[] channels )

        Params:
            writer = the writer which is used to write data to the DMQ
            checker = the checker which is used to check the data which the
                writer wrote to the DMQ

    ***************************************************************************/

    public this ( IWriter writer, IChecker checker )
    {
        super(writer.test_channels);
        this.writer = writer;
        this.checker = checker;
    }

    /***************************************************************************

        Returns:
            automatic description based on the names of the writer and the
            checker.

    ***************************************************************************/

    public override Description description ( )
    {
        Description desc;
        desc.name = classname(this.writer) ~ `/` ~
            classname(this.checker)  ~ " test";
        return desc;
    }

    /***************************************************************************

        Initializes the writer and checker to the internal DMQ client.

    ***************************************************************************/

    override public void prepare ( )
    {
        super.prepare();
        this.writer.prepare(this.dmq);
        this.checker.prepare(this.dmq);
    }

    /***************************************************************************

        Runs the Writer, which pushes or produces `this.records` to all DMQ
        channels used in the test, then the Checker, which pops or consumes from
        the same channels and checks if the data received from the DMQ matches
        `this.records`.

    ***************************************************************************/

    override public void run ( )
    {
        this.writer.run(this.records);
        this.checker.check(this.records);
    }
}

/*******************************************************************************

    Class for test cases which work via a writer and a checker. The writer and
    the checker are run in parallel, in two separate tasks.

*******************************************************************************/

class ParallelCheckedDmqTestCase : DmqTestCase
{
    import ocean.io.select.EpollSelectDispatcher;
    import ocean.text.util.ClassName;

    /***************************************************************************

        Common code for writer/checker tasks.

    ***************************************************************************/

    private abstract class TestTask: Task
    {
        /***********************************************************************

            DMQ client used by the writer/checker. Each must have a separate DMQ
            client as the client is bound to a single task. The writer and
            checker run in different tasks, so need separate DMQ clients.

        ***********************************************************************/

        protected DmqClient dmq;

        /***********************************************************************

            Exception caught during running the writer/checker.

        ***********************************************************************/

        public Exception e;

        /***********************************************************************

            Prepares the test runner and connects to the DMQ node.
            Should be called before calling `this.go()`.

        ***********************************************************************/

        public void prepare ( )
        {
            this.dmq = new DmqClient();

            // The test needs to ensure that the DMQ client of the writer and
            // checker are both connected, *before* the actual tests begin. The
            // easiest way to do this is to block the main test task when
            // connecting.
            this.dmq.addNode("127.0.0.1", 10000);
            this.dmq.neo.connect(this.outer.test_case_task);
        }

        /***********************************************************************

            The task method. Calls the abstract go_() and handles exceptions.
            Also, uses the outer class member this.outer.running to count the
            number of writer/checker tasks which are currently running. When
            this count reaches 0, the test task is resumed.

        ***********************************************************************/

        public override void run ( )
        {
            this.outer.running++;
            scope ( exit )
            {
                this.dmq.neo.shutdown();

                if ( --this.outer.running == 0 )
                    this.outer.test_case_task.resume();
            }

            try
            {
                this.go_();
            }
            catch ( Exception e )
            {
                this.e = e;
            }
        }

        /***********************************************************************

            Abstract method which performs the actual writer/checker logic.

        ***********************************************************************/

        abstract protected void go_ ( );
    }

    /***************************************************************************

        Task running a Writer instance.

    ***************************************************************************/

    private class WriteTask : TestTask
    {
        /***********************************************************************

            Writer instance.

        ***********************************************************************/

        private IWriter writer;

        /***********************************************************************

            Constructor.

            Params:
                writer = the writer which is used to write data to the DMQ

        ***********************************************************************/

        public this ( IWriter writer )
        {
            this.writer = writer;
        }

        /***********************************************************************

            Prepares the test runner and connects to the DMQ node.
            Should be called before calling `this.go()`.

        ***********************************************************************/

        override public void prepare ( )
        {
            super.prepare();
            this.writer.prepare(this.dmq);
        }

        /***********************************************************************

            Runs the writer logic, suspending this.task for async I/O.

        ***********************************************************************/

        override protected void go_ ( )
        {
            this.writer.run(this.outer.records);
        }
    }

    /***************************************************************************

        Task running a Checker instance.

    ***************************************************************************/

    private class CheckTask : TestTask
    {
        /***********************************************************************

            Checker instance.

        ***********************************************************************/

        private IChecker checker;

        /***********************************************************************

            Constructor.

            Params:
                checker = the checker which is used to check the data which the
                          writer wrote to the DMQ

        ***********************************************************************/

        public this ( IChecker checker )
        {
            this.checker = checker;
        }

        /***********************************************************************

            Prepares the test runner and connects to the DMQ node.
            Should be called before calling `this.go()`.

        ***********************************************************************/

        override public void prepare ( )
        {
            super.prepare();
            this.checker.prepare(this.dmq);
        }

        /***********************************************************************

            Runs the checker logic, suspending this.task for async I/O.

        ***********************************************************************/

        override protected void go_ ( )
        {
            this.checker.check(this.outer.records);
        }
    }

    /***************************************************************************

        Task running a Writer instance.

    ***************************************************************************/

    private WriteTask write_task;

    /***************************************************************************

        Task running a Checker instance.

    ***************************************************************************/

    private CheckTask check_task;

    /***************************************************************************

        Count of the number of TestTasks running. Used in TestTask.run().

    ***************************************************************************/

    private uint running;

    /***************************************************************************

        Test case's task

    ***************************************************************************/

    private Task test_case_task;

    /***************************************************************************

        Constructor.

        Params:
            writer = the writer which is used to write data to the DMQ
            checker = the checker which is used to check the data which the
                writer wrote to the DMQ

    ***************************************************************************/

    public this ( IWriter writer, IChecker checker )
    {
        super(writer.test_channels);

        this.test_case_task = Task.getThis();
        assert(this.test_case_task !is null);

        this.write_task = this.new WriteTask(writer);
        this.check_task = this.new CheckTask(checker);
    }

    /***************************************************************************

        Returns:
            automatic description based on the names of the writer and the
            checker.

    ***************************************************************************/

    public override Description description ( )
    {
        Description desc;
        desc.name = classname(this.write_task.writer) ~ `/` ~
            classname(this.check_task.checker)  ~ " parallel test";
        return desc;
    }

    /***************************************************************************

        Prepares the test runner tasks and connects to the DMQ node.

    ***************************************************************************/

    override public void prepare ( )
    {
        super.prepare();
        this.write_task.prepare();
        this.check_task.prepare();
    }

    /***************************************************************************

        Runs the writer and checker in parallel.
        The writer pushes or produces `this.records` to all DMQ channels used in
        the test while the pops or consumes from the same channels and checks if
        the data received from the DMQ matches `this.records`.

    ***************************************************************************/

    override public void run ( )
    {
        theScheduler.schedule(this.write_task);
        theScheduler.schedule(this.check_task);

        this.test_case_task.suspend();

        if ( this.write_task.e ) throw this.write_task.e;
        if ( this.check_task.e ) throw this.check_task.e;
    }
}

/*******************************************************************************

    Channel subscriber test.

*******************************************************************************/

class SubscribeDmqTestCase : DmqTestCase
{
    import dmqtest.cases.writers.model.IWriter;
    import ocean.text.util.ClassName;

    /***************************************************************************

        DMQ consumers.

    ***************************************************************************/

    private DmqClient.Neo.Consumers consumers;

    /***************************************************************************

        The writer which is used to write data to the DMQ.

    ***************************************************************************/

    private IWriter writer;

    /***************************************************************************

        Constructor.

        Params:
            writer = the writer which is used to write data to the DMQ

    ***************************************************************************/

    public this ( IWriter writer )
    {
        super(writer.test_channels);
        this.writer = writer;
    }

    /***************************************************************************

        Initializes internal DMQ client and consumers.

    ***************************************************************************/

    override public void prepare ( )
    {
        super.prepare();
        this.writer.prepare(this.dmq);
        this.consumers = this.dmq.neo.new Consumers;
    }

    /***************************************************************************

        Returns:
            the test description.

    ***************************************************************************/

    override public Description description ( )
    {
        Description desc;
        desc.name = "Channel subscribers/" ~ classname(this.writer);
        return desc;
    }

    /***************************************************************************

        Runs the test.

    ***************************************************************************/

    override public void run ( )
    {

        /*
         * Push a bunch of records to the queue channel.
         */
        Const!(char[])[] pushed_records = this.records;
        this.writer.run(pushed_records);

        /*
         * Pop one record, should succeed.
         */
        this.pop1Success(pushed_records[0]);
        pushed_records = pushed_records[1 .. $];

        /*
         * Assign multiple subscribers to the channel. The "sub1" subscribers
         * should share the records already in the channel while "sub2" gets
         * nothing.
         */
        this.startConsumers("sub1", "sub1", "sub2");

        /*
         * Attempt to pop one record, should fail with
         * `channel_has_subscribers`.
         */
        this.pop1FailureSubscribed();

        /*
         * Consume records, expecting only the "sub1" subscriber to receive the
         * remaining records.
         */
        this.consume(pushed_records, "sub1");

        /*
         * Push another bunch of records to the queue channel that now has
         * subscribers. All subscribers should now receive the records.
         */
        this.writer.run(this.records);
        this.consume(this.records, "sub1", "sub2");

        this.consumers.stop();
    }

    /***************************************************************************

        Pops one record from all channels of the queue, expecting it to match
        `expected_record`.

        Params:
            expected_record = the expected record

    ***************************************************************************/

    private void pop1Success ( cstring expected_record )
    {
        foreach (channel; this.channels)
        {
            auto record = cast(char[])this.dmq.neo.pop(channel);
            test!("==")(record, expected_record);
        }
    }

    /***************************************************************************

        Pops one record from each channel, expecting all the Pop requests to
        fail because the channel has subscribers.

    ***************************************************************************/

    private void pop1FailureSubscribed ( )
    {
        foreach (channel; this.channels)
        {
            uint[DmqClient.Neo.PopNotificationType.max + 1] notifications;

            test!("==")(this.dmq.neo.pop(channel, notifications).length, 0);

            with (DmqClient.Neo.PopNotificationType)
            {
                test!("==")(notifications[empty], 0);
                test!("==")(notifications[received], 0);
                test!("==")(notifications[not_connected], 0);
                test!("!=")(notifications[failure], 0);
                test!("==")(notifications[node_disconnected], 0);
                test!("!=")(notifications[channel_has_subscribers], 0);
                test!("==")(notifications[node_error], 0);
                test!("==")(notifications[unsupported], 0);
            }
        }
    }

    /***************************************************************************

        Starts `subscriber_names.length` consumers. Each consumer uses the
        corresponding subscriber name. The consumers are started in the order of
        `subscriber_names`. Duplicate subscriber names are allowed to have
        multiple consumers sharing a queue.

        Params:
            subscriber_names = the subscriber names to use, one per consumer

    ***************************************************************************/

    private void startConsumers ( Const!(char[])[] subscriber_names ... )
    {
        foreach (channel; this.channels)
        {
            foreach (subscriber_name; subscriber_names)
                this.consumers.startConsumer(channel, subscriber_name);
        }
    }

    /***************************************************************************

        Consumes records from the queue after consumers have been started via
        `startConsumers()`. Checks the received records, expecting that all
        subscribers to all channels receive `expected_records`. This is the case
        when
          1. for each channel and subscriber name one or multiple consumers are
             started,
          2. the channels are empty and
          3. `expected_records` are pushed to all channels.

        Params:
            expected_records = the records expected to be received for each
                channel and subscriber
            subscriber_names = the names of all subscribers (duplicates are not
                allowed)

    ***************************************************************************/

    private void consume ( Const!(char[])[] expected_records,
        Const!(char[])[] subscriber_names ... )
    in
    {
        assert(expected_records.length);
        assert(subscriber_names.length);
    }
    body
    {
        Const!(char[])[][Const!(char[])][Const!(char[])]
            records_by_channel_and_subscriber;

        foreach (channel; this.channels)
        {
            Const!(char[])[][Const!(char[])] records_by_subscriber;
            foreach (subscriber_name; subscriber_names)
            {
                assert(!(subscriber_name in records_by_subscriber),
                    "duplicate subscriber name");
                records_by_subscriber[subscriber_name] = null;
            }
            records_by_channel_and_subscriber[channel] = records_by_subscriber;
        }

        auto n_expected_records =
            expected_records.length * subscriber_names.length *
            this.channels.length;
        uint popped;
        for (popped = 0; popped < n_expected_records;)
        {
            // Wait for something to happen
            this.consumers.waitNextEvent();

            // Append received records to records_by_channel_and_subscriber
            foreach (record; this.consumers.received_records)
            {
                test!(">")(records_by_channel_and_subscriber.length, 0);
                Const!(char[])[][Const!(char[])]* records_by_subscriber =
                    record.channel in records_by_channel_and_subscriber;
                test!("!is")(records_by_subscriber, null);
                test!(">")(records_by_subscriber.length, 0);
                Const!(char[])[]* records = record.subscriber in *records_by_subscriber;
                test!("!is")(records, null);
                test!("<")(records.length, expected_records.length);
                (*records) ~= record.value;
                popped++;
            }

            this.consumers.received_records.length = 0;
            enableStomping(this.consumers.received_records);
        }

        test!("==")(popped, n_expected_records);

        foreach (channel, records_by_subscriber; records_by_channel_and_subscriber)
            foreach (subscriber, records; records_by_subscriber)
                foreach (i, record; sort(records))
                    test!("==")(record, expected_records[i]);
    }
}
