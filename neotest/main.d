/*******************************************************************************

    Copyright:
        Copyright (c) 2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module test.neotest.main;

import ocean.meta.types.Qualifiers;
import ocean.io.Stdout;

import ocean.task.Scheduler;
import ocean.task.Task;

import ocean.io.select.client.TimerEvent;
import dmqproto.client.DmqClient;

abstract class DmqTest
{
    import swarm.neo.AddrPort;
    import swarm.neo.authentication.HmacDef: Key;

    protected DmqClient dmq;

    public this ( )
    {
        SchedulerConfiguration config;
        initScheduler(config);

        auto auth_name = "neotest";
        ubyte[] auth_key = Key.init.content;
        this.dmq = new DmqClient(theScheduler.epoll, auth_name, auth_key,
            &this.connNotifier);
        this.dmq.neo.addNode("78.46.85.196", 10_001);
    }

    final public void start ( )
    {
        theScheduler.eventLoop();
    }
    private void connNotifier ( DmqClient.Neo.ConnNotification info )
    {
        with (info.Active) switch (info.active)
        {
        case connected:
            Stdout.formatln("Connected. Let's Go...................................................................");
            this.go();
            break;
        case error_while_connecting:
            with (info.error_while_connecting)
            {
                Stderr.formatln("Connection error: {}", e.message);
                return;
            }
        default:
            assert(false);
        }
    }

    abstract protected void go ( );

    protected void popNotifier ( DmqClient.Neo.Pop.Notification info,
        const(DmqClient.Neo.Pop.Args) args )
    {
        with ( info.Active ) switch ( info.active )
        {
            case received:
                Stdout.formatln("Pop {} received {}.", args.channel,
                    info.received.value);
                break;

            case empty:
                Stdout.formatln("Pop {} received nothing -- channel empty.", args.channel);
                break;

            case not_connected:
                Stdout.formatln("Pop {} no connected nodes to pop from.", args.channel);
                break;

            case failure:
                Stdout.formatln("Pop {} failed on all nodes.", args.channel);
                break;

            case node_disconnected:
                Stdout.formatln("Pop {} failed due to connection error {} on {}:{}",
                    args.channel,
                    info.node_disconnected.e.message(),
                    info.node_disconnected.node_addr.address_bytes,
                    info.node_disconnected.node_addr.port);
                break;

            case node_error:
                Stdout.formatln("Pop {} failed due to a node error on {}:{}",
                    args.channel,
                    info.node_error.node_addr.address_bytes,
                    info.node_error.node_addr.port);
                break;

            case unsupported:
                switch ( info.unsupported.type )
                {
                    case info.unsupported.type.RequestNotSupported:
                        Stdout.formatln("Consume {} node {}:{} does not support this request",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;
                    case info.unsupported.type.RequestVersionNotSupported:
                        Stdout.formatln("Consume {} node {}:{} does not support this request version",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;

                    default: assert(false);
                }
                break;

            default: assert(false);
        }
    }

    protected void pushNotifier ( DmqClient.Neo.Push.Notification info,
        const(DmqClient.Neo.Push.Args) args )
    {
        with ( info.Active ) switch ( info.active )
        {
            case success:
                break;

            case failure:
                Stdout.formatln("Push {}:{} failed on all nodes.",
                    args.channels, cast(cstring)args.value);
                break;

            case node_disconnected:
                Stdout.formatln("Push {}:{} failed due to connection error {} on {}:{}",
                    args.channels, cast(cstring)args.value,
                    info.node_disconnected.e.message(),
                    info.node_disconnected.node_addr.address_bytes,
                    info.node_disconnected.node_addr.port);
                break;

            case node_error:
                Stdout.formatln("Push {}:{} failed due to a node error on {}:{}",
                    args.channels, cast(cstring)args.value,
                    info.node_error.node_addr.address_bytes,
                    info.node_error.node_addr.port);
                break;

            case unsupported:
                switch ( info.unsupported.type )
                {
                    case info.unsupported.type.RequestNotSupported:
                        Stdout.formatln("Push {} node {}:{} does not support this request",
                            args.channels,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;
                    case info.unsupported.type.RequestVersionNotSupported:
                        Stdout.formatln("Push {} node {}:{} does not support this request version",
                            args.channels,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;

                    default: assert(false);
                }
                break;

            default: assert(false);
        }
    }

    protected void consumeNotifier ( DmqClient.Neo.Consume.Notification info,
        const(DmqClient.Neo.Consume.Args) args )
    {
        with ( info.Active ) switch ( info.active )
        {
            case received:
                Stdout.formatln("Consumed: {}", cast(cstring)info.received.value);
                break;

            case stopped:
                Stdout.formatln("Consume {} stopped on all nodes.",
                    args.channel);
                break;

            case channel_removed:
                Stdout.formatln("Consume {} channel removed.",
                    args.channel);
                break;

            case node_disconnected:
                Stdout.formatln("Consume {} failed due to connection error {} on {}:{}",
                    args.channel,
                    info.node_disconnected.e.message(),
                    info.node_disconnected.node_addr.address_bytes,
                    info.node_disconnected.node_addr.port);
                break;

            case node_error:
                Stdout.formatln("Consume {} failed due to a node error on {}:{}",
                    args.channel,
                    info.node_error.node_addr.address_bytes,
                    info.node_error.node_addr.port);
                break;

            case unsupported:
                switch ( info.unsupported.type )
                {
                    case info.unsupported.type.RequestNotSupported:
                        Stdout.formatln("Consume {} node {}:{} does not support this request",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;
                    case info.unsupported.type.RequestVersionNotSupported:
                        Stdout.formatln("Consume {} node {}:{} does not support this request version",
                            args.channel,
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        break;

                    default: assert(false);
                }
                break;

            default: assert(false);
        }
    }
}

class TaskPushTest : DmqTest
{
    class PushTask : Task
    {
        import ocean.task.util.Timer;

        override public void run ( )
        {
            while ( true )
            {
                this.outer.dmq.blocking.push("test".dup, "whatever".dup);
                wait(1_000_000);
            }
        }
    }

    override protected void go ( )
    {
        theScheduler.schedule(new PushTask);
    }
}

class TaskPopTest : DmqTest
{
    class PopTask : Task
    {
        import ocean.task.util.Timer;

        override public void run ( )
        {
            void[] value;

            while ( true )
            {
                auto res = this.outer.dmq.blocking.pop("test".dup, value);
                if ( res.succeeded )
                    Stdout.formatln("Popped '{}'", res.value);

                wait(1_000_000);
            }
        }
    }

    override protected void go ( )
    {
        theScheduler.schedule(new PopTask);
    }
}

class PopTest : DmqTest
{
    private TimerEvent timer;

    this ( )
    {
        this.timer = new TimerEvent(&this.timer_dg);
    }

    protected override void go ( )
    {
        this.timer.set(0,500,0,500);
        theScheduler.epoll.register(this.timer);
    }

    private bool timer_dg ( )
    {
        dmq.neo.pop("test".dup, &this.popNotifier);

        return true;
    }
}

class PushTest : DmqTest
{
    private TimerEvent timer;

    this ( )
    {
        this.timer = new TimerEvent(&this.timer_dg);
    }

    protected override void go ( )
    {
        this.timer.set(0,1,0,1);
        theScheduler.epoll.register(this.timer);
    }

    private bool timer_dg ( )
    {
        auto value = "hello node".dup;
        dmq.neo.push(["test".dup, "test2".dup], value, &this.pushNotifier);

        return true;
    }
}

class ConsumeTest : DmqTest
{
    protected override void go ( )
    {
        Stdout.formatln("Starting Consume...");
        dmq.neo.consume("test".dup, &this.consumeNotifier);
    }
}

class PushConsumeTest : DmqTest
{
    private TimerEvent timer;
    private bool test_suspension;
    private uint counter;
    private uint resumes;       // if this.test_suspension, used to stop the
                                // request after 3 suspend/resume cycles

    private enum State
    {
        SuspendSoon,
        ResumeSoon,
        StopSoon
    }
    private State state;


    this ( bool test_suspension )
    {
        this.test_suspension = test_suspension;
        this.timer = new TimerEvent(&this.timer_dg);
    }

    import swarm.neo.protocol.Message: RequestId;
    RequestId consume_id;

    // Sets up:
    // 1. a timer that fires 4 time a second and Pushes to the test channel
    // 2. a Consume request on the same channel (which, if this.test_suspension,
    //    will be suspended/resumed periodically and stopped after a while)
    protected override void go ( )
    {
        Stdout.formatln("Starting Consume...");
        this.consume_id = dmq.neo.consume("test".dup, &this.consumeNotifier);

        this.timer.set(0,250,0,250);
        theScheduler.epoll.register(this.timer);
    }

    private bool timer_dg ( )
    {
        auto value = "hello node".dup;
        dmq.neo.push("test".dup, value, &this.pushNotifier);

        if ( this.test_suspension )
        {
            if ( ++this.counter >= 10 )
            {
                switch ( this.state )
                {
                    case State.SuspendSoon:
                        dmq.neo.control(this.consume_id,
                            ( DmqClient.Neo.Consume.IController consume )
                            {
                                Stdout.yellow.formatln(
                                    "**************************** Suspending ****************************\n" ~
                                    "consume.suspend() => {}", consume.suspend()).default_colour;
                            }
                        );
                        this.state = State.ResumeSoon;
                        break;

                    case State.ResumeSoon:
                        dmq.neo.control(this.consume_id,
                            ( DmqClient.Neo.Consume.IController consume )
                            {
                                Stdout.yellow.formatln("**************************** Resuming ****************************\n"~
                                "consume.resume() => {}", consume.resume()).default_colour;
                            }
                        );
                        this.state = ++this.resumes >= 4
                            ? State.StopSoon : State.SuspendSoon;
                        break;

                    case State.StopSoon:
                        dmq.neo.control(this.consume_id,
                            ( DmqClient.Neo.Consume.IController consume )
                            {
                                Stdout.yellow.formatln(
                                "**************************** Stopping ****************************\n" ~
                                "consume.stop() => {}", consume.stop()).default_colour;
                            }
                        );
                        break;
                    default: assert(false);
                }
                this.counter = 0;
            }
        }

        return true;
    }
}

void main ( char[][] args )
{
    if ( args.length != 2 )
        throw new Exception("Expected exactly one CLI argument.");

    DmqTest app;
    switch ( args[1] )
    {
        case "pop":
            app = new PopTest;
            break;
        case "task-pop":
            app = new TaskPopTest;
            break;
        case "push":
            app = new PushTest;
            break;
        case "task-push":
            app = new TaskPushTest;
            break;
        case "consume":
            app = new ConsumeTest;
            break;
        case "pushconsume":
            app = new PushConsumeTest(false);
            break;
        case "pushconsumesuspend":
            app = new PushConsumeTest(true);
            break;
        default:
            throw new Exception("Unknown request type.");
    }
    app.start();
}
