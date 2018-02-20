/*******************************************************************************

    Provides global test client instance used from test cases to access
    the node.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.DmqClient;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.util.log.Logger;

/*******************************************************************************

    Class that encapsulates fiber/epoll reference and provides
    functions to emulate blocking API for swarm DMQ client.

*******************************************************************************/

class DmqClient
{
    import ocean.core.Enforce;
    import ocean.core.Array : copy;
    import ocean.task.Task;
    import ocean.task.Scheduler;
    import ocean.io.select.fiber.SelectFiber;

    static import dmqproto.client.DmqClient;
    import swarm.client.plugins.ScopeRequests;
    import swarm.neo.authentication.HmacDef: Key;

    /***************************************************************************

        Helper class to perform a request and suspend the current task until the
        request is finished.

    ***************************************************************************/

    private final class TaskBlockingRequest
    {
        import ocean.io.select.protocol.generic.ErrnoIOException : IOError;
        import swarm.Const : NodeItem;

        /// Task instance to be suspended / resumed while the request is handled
        private Task task;

        /// Counter of the number of request-on-conns which are not finished.
        private uint pending;

        /// Flag per request-on-conn, set to true if it is queued. Used to
        /// ensure that pending is not incremented twice.
        private bool[NodeItem] queued;

        /// Set if an error occurs in any request-on-conn.
        private bool error;

        /// Stores the last error message.
        private mstring error_msg;

        /***********************************************************************

            Constructor. Sets this.task to the current task.

        ***********************************************************************/

        public this ( )
        {
            this.task = Task.getThis();
            assert(this.task !is null);
        }

        /***********************************************************************

            Should be called after assigning a request. Suspends the task until
            the request finishes and then checks for errors.

            Throws:
                if an error occurred while handling the request

        ***********************************************************************/

        public void wait ( )
        {
            if ( this.pending > 0 )
                this.task.suspend();
            enforce(!this.error, idup(this.error_msg));
        }

        /***********************************************************************

            DMQ request notifier to pass to the request being assigned.

            Params:
                info = notification info

        ***********************************************************************/

        public void notify ( dmqproto.client.DmqClient.IRequestNotification info )
        {
            switch ( info.type )
            {
                case info.type.Queued:
                    this.queued[info.nodeitem] = true;
                    this.pending++;
                    break;

                case info.type.Started:
                    if ( !(info.nodeitem in this.queued) )
                        this.pending++;
                    break;

                case info.type.Finished:
                    if ( !info.succeeded )
                    {
                        info.message(this.error_msg);

                        if ( cast(IOError) info.exception )
                            this.outer.log.warn("Socket I/O failure : {}",
                                this.error_msg);
                        else
                            this.error = true;
                    }

                    if ( --this.pending == 0 && this.task.suspended() )
                        this.task.resume();
                    break;

                default:
            }
        }
    }

    /*******************************************************************************

        Reference to common fakedht logger instance

    *******************************************************************************/

    private Logger log;

    /***************************************************************************

        Alias for type of the standard DMQ client.

    ***************************************************************************/

    alias dmqproto.client.DmqClient.DmqClient RawClient;

    /***************************************************************************

        Shared DMQ client instance.

    ***************************************************************************/

    private RawClient raw_client;

    /***************************************************************************

        Wrapper class containing all neo requests. (When the legacy requests are
        removed, the content of this class will be moved to the top level.)

    ***************************************************************************/

    private class Neo
    {
        /***********************************************************************

            Pop notification type enumerator.

        ***********************************************************************/

        public alias RawClient.Neo.Pop.Notification.Active PopNotificationType;

        /***********************************************************************

            Flag which is set (by connect()) when a connection error occurs.

        ***********************************************************************/

        private bool connection_error;

        /***********************************************************************

            Task that should be blocked on connect

        ***********************************************************************/

        private Task connect_task;

        /***********************************************************************

            Waits until either neo connections to all nodes have been
            established (including authentication) or one connection has failed.

        ***********************************************************************/

        public void connect ( )
        {
            this.connect(Task.getThis());
        }

        /***********************************************************************

            Waits until either neo connections to all nodes have been
            established (including authentication) or one connection has failed.

            Params:
                task = task to block while connecting (this allows another task
                       to be blocked, not the task issuing this connect)

        ***********************************************************************/

        public void connect ( Task connect_task )
        {
            this.connect_task = connect_task;
            assert (this.connect_task !is null);

            scope stats = this.outer.raw_client.neo.new Stats;

            this.connection_error = false;
            while (stats.num_connected_nodes < stats.num_registered_nodes &&
                    !this.connection_error )
            {
                this.connect_task.suspend();
            }
        }

        /***********************************************************************

            Connection notifier used by the client (see the outer class' ctor).

            Params:
                node_address = address/port of node which notification refers to
                e = exception instance indicating an error (null indicates
                    connection success)

        ***********************************************************************/

        private void connectionNotifier ( RawClient.Neo.ConnNotification info )
        {
            with (info.Active) switch (info.active)
            {
            case connected:
                log.trace("Neo connection established (on {}:{})",
                    info.connected.node_addr.address_bytes,
                    info.connected.node_addr.port);
                break;
            case error_while_connecting:
                with (info.error_while_connecting)
                {
                    this.connection_error = true;
                    log.error("Neo connection error: {} (on {}:{})",
                            getMsg(e),
                            node_addr.address_bytes, node_addr.port);
                }
                break;
            default:
                assert(false);
            }

            if (this.connect_task && this.connect_task.suspended())
                this.connect_task.resume();
        }

        /***********************************************************************

            Shuts down the neo client, closing all established connections.

        ***********************************************************************/

        public void shutdown ( )
        {
            this.neo_client.shutdown();
        }

        /***********************************************************************

            Performs a neo Push request, suspending the fiber until it is done.

            Params:
                channels = channels to push to
                data = record value to push

            Throws:
                upon failure

        ***********************************************************************/

        public void push ( Const!(char[])[] channels, cstring data )
        {
            auto res = this.outer.raw_client.blocking.push(channels.dup, data);

            enforce(res.succeeded, "Neo Push request failed on all nodes");
        }

        /***********************************************************************

            Performs a neo Pop request, suspending the fiber until it is done.

            Params:
                channel = channel to push to

            Returns:
                popped value or empty string, if the channel was empty

            Throws:
                upon failure

        ***********************************************************************/

        public void[] pop ( cstring channel )
        {
            void[] result;
            bool error = false;

            auto res = this.outer.raw_client.blocking.pop(channel, result);

            enforce(res.succeeded, "Neo Pop request failed on all nodes");
            return res.value;
        }

        /***********************************************************************

            Performs a neo Pop request, suspending the fiber until it is done.
            Increments the corresponding element of `notifications` for each
            notification. For example, if three "node disconnected"
            notifications are reported then
            `notifications[PopNotificationType.node_disconnected]` will be
            incremented by 3.

            Params:
                channel = channel to push to
                notifications = notification counters, the length must be
                    greater than `PopNotificationType.max`

            Returns:
                the popped value or an empty string if either the channel was
                empty or the request failed on all nodes

        ***********************************************************************/

        public void[] pop ( cstring channel, uint[] notifications )
        in
        {
            assert(notifications.length > PopNotificationType.max);
        }
        body
        {
            void[] result;

            void notify ( RawClient.Neo.Pop.Notification info,
                RawClient.Neo.Pop.Args args )
            {
                notifications[info.active]++;
            }

            this.outer.raw_client.blocking.pop(channel, result, &notify);

            return result;
        }

        /***********************************************************************

            Class wrapping one or more Consume requests. Multiple Consume
            requests to be handled concurrently should be initiated via an
            instance of this class, so that messages received from the single
            node connection can be demultiplexed in parallel, without one
            Consume causing others to block.

        ***********************************************************************/

        public class Consumers
        {
            /*******************************************************************

                Struct containing the details of a received record.

            *******************************************************************/

            public static struct ReceivedRecord
            {
                istring subscriber;
                istring channel;
                istring value;
            }

            /*******************************************************************

                List of records received since the last call to waitNextEvent().

            *******************************************************************/

            public ReceivedRecord[] received_records;

            /*******************************************************************

                Task bounded to this instance.

            *******************************************************************/

            private Task task;

            /*******************************************************************

                Indicator if the task is waiting on data and should be resumed

            *******************************************************************/

            private bool waiting;

            /*******************************************************************

                Set of Consume request ids for the same channel and subscriber
                name.

            *******************************************************************/

            private struct RequestIds
            {
                /***************************************************************

                    The Consume request ids for the same channel and subscriber
                    name.

                ***************************************************************/

                private RawClient.Neo.RequestId[] ids;

                /***************************************************************

                    The number of active request ids in `ids`. This value is
                    incremented in `add` and decremented in `remove`.

                ***************************************************************/

                private uint n;

                /***************************************************************

                    Sanity check.

                ***************************************************************/

                invariant ( )
                {
                    assert((&this).n <= (&this).ids.length);
                }

                /***************************************************************

                    Adds `id` to the set of request ids and increments the
                    counter of active request ids.

                    Params:
                        id = the request id to add

                ***************************************************************/

                public void add ( RawClient.Neo.RequestId id )
                {
                    (&this).ids ~= id;
                    (&this).n++;
                }

                /***************************************************************

                    Decrements the counter of active request ids.

                    Returns:
                        the number of remaining active request ids, which is 0
                        if the last active request id was removed.

                ***************************************************************/

                public uint remove ( )
                in
                {
                    assert((&this).n);
                }
                body
                {
                    if (!--(&this).n)
                        delete (&this).ids;

                    return (&this).n;
                }

                /***************************************************************

                    Returns:
                        the array of request ids.

                ***************************************************************/

                public Const!(RawClient.Neo.RequestId)[] opSlice ( )
                {
                    return (&this).ids;
                }

                /***************************************************************

                    Creates a new instance of this struct initialised to contain
                    one active request id which is `id`.

                    Params:
                        id = the first active request id

                    Returns:
                        the new instance of this struct.

                ***************************************************************/

                static public typeof(*(&this)) create ( RawClient.Neo.RequestId id )
                out (instance)
                {
                    assert(&instance);
                }
                body
                {
                    return typeof(*(&this))([id], 1);
                }
            }

            /*******************************************************************

                Set of active Consume request ids, indexed by channel and
                subscriber name

            *******************************************************************/

            private RequestIds[istring][istring] request_ids;

            /*******************************************************************

                Struct wrapping possible Consume error flags

            *******************************************************************/

            private static struct Errors
            {
                bool stopped;
                uint suspended;
                uint resumed;
                bool channel_removed;
                bool disconnection;
                bool node_error;
            }

            /*******************************************************************

                Consume error flags, set by the notifier

            *******************************************************************/

            private Errors errors;

            /*******************************************************************

                Starts consuming from the specified channel.
                If a non-empty subscriber name is specified then multiple
                consumers can be added to the same channel with the same
                subscriber name.

                Params:
                    channel = name of the channel to consume from
                    subscriber = channel subscriber name

            *******************************************************************/

            public void startConsumer ( cstring channel, cstring subscriber = "" )
            in
            {
                if (!subscriber.length)
                {
                    if (auto channels = "" in this.request_ids)
                    {
                        assert(!(channel in *channels), "A Consume for this "
                            ~ "channel is already active");
                    }
                }
            }
            body
            {
                auto id = this.outer.neo_client.consume(
                    channel.dup, &this.notifier,
                    neo_client.Subscriber(subscriber.dup)
                );
                auto ichn = idup(channel);
                auto isub = idup(subscriber);

                if (auto subscribers = ichn in this.request_ids)
                {
                    if (RequestIds* ids = isub in *subscribers)
                    {
                        ids.add(id);
                    }
                    else
                    {
                        (*subscribers)[isub] = RequestIds.create(id);
                    }
                }
                else
                {
                    this.request_ids[ichn] = [isub: RequestIds.create(id)];
                }
            }

            /*******************************************************************

                Returns when either data has been received by the Consume request
                or the channel being consumed has been removed. If neither of
                those things are immediately true, when the method is called,
                the bound task is suspended until one of them occurs. Thus, when
                this method returns, one of the following has happened:

                1. Data was already available (in this.data), so the task
                   was not suspended.
                2. The task was suspended, new data arrived, and the task
                   was resumed. The new data is added to this.data where it
                   can be read by the test case. When the test case has finished
                   checking the received data, it must remove it from the array,
                   otherwise subsequent calls to waitNextEvent() will
                   always return immediately (case 1), without waiting.
                3. The task was suspended, the Consume request terminated due
                   to the channel being removed, and the task was resumed.
                   It is not possible to make further use of this instance.

            *******************************************************************/

            public void waitNextEvent ( )
            {
                if (this.received_records.length)
                    return;

                this.errors = this.errors.init;
                this.waiting = true;

                this.task = Task.getThis();
                this.task.suspend();

                this.waiting = false;

                enforce(!this.errors.stopped,
                    "Stopped during Consume request");
                enforce(!this.errors.suspended,
                    "Suspend during Consume request");
                enforce(!this.errors.resumed,
                    "Resume during Consume request");
                enforce(!this.errors.channel_removed,
                    "Channel removed during Consume request");
                enforce(!this.errors.disconnection,
                    "Disconnection during Consume request");
                enforce(!this.errors.node_error,
                    "Node error during Consume request");
            }


            /*******************************************************************

                Stops all active Consume requests, blocking the bound fiber
                until the client has received notification that they are
                terminated.

            *******************************************************************/

            public void stop ( )
            {
                // Tell Consumes to stop.
                foreach ( subscriber_ids; this.request_ids )
                {
                    foreach ( ids; subscriber_ids )
                    {
                        foreach (id; ids[])
                            this.outer.neo_client.control(id,
                                ( RawClient.Neo.Consume.IController controller )
                                {
                                    controller.stop();
                                }
                            );
                    }
                }

                // Wait until they are all stopped.
                do
                {
                    this.errors = this.errors.init;
                    this.waiting = true;
                    this.task = Task.getThis();
                    this.task.suspend();
                    this.waiting = false;

                    enforce(this.errors.stopped,
                        "Missing stop during Consume request");
                    enforce(!this.errors.suspended,
                        "Suspend during Consume request");
                    enforce(!this.errors.resumed,
                        "Resume during Consume request");
                    enforce(!this.errors.channel_removed,
                        "Channel removed during Consume request");
                    enforce(!this.errors.disconnection,
                        "Disconnection during Consume request");
                    enforce(!this.errors.node_error,
                        "Node error during Consume request");
                }
                while ( this.request_ids.length );
            }

            /*******************************************************************

                Suspends all active Consume requests, blocking the bound fiber
                until the client has received notification that they are
                suspended.

            *******************************************************************/

            public void suspend ( )
            {
                // Tell Consumes to suspend.
                foreach ( subscriber_ids; this.request_ids )
                {
                    foreach ( ids; subscriber_ids )
                    {
                        foreach ( id; ids[] )
                            this.outer.neo_client.control(id,
                                ( RawClient.Neo.Consume.IController controller )
                                {
                                    controller.suspend();
                                }
                            );
                    }
                }

                // Wait until they are all suspended.
                uint suspended;
                do
                {
                    this.errors = this.errors.init;
                    this.waiting = true;
                    this.task = Task.getThis();
                    this.task.suspend();
                    this.waiting = false;


                    enforce(!this.errors.stopped,
                        "Stop during Consume request");
                    enforce(!this.errors.channel_removed,
                        "Channel removed during Consume request");
                    enforce(!this.errors.disconnection,
                        "Disconnection during Consume request");
                    enforce(!this.errors.node_error,
                        "Node error during Consume request");

                    suspended += this.errors.suspended;
                }
                while ( suspended < this.request_ids.length );
            }

            /*******************************************************************

                Resumes all active Consume requests, blocking the bound fiber
                until the client has received notification that they are
                resumed.

            *******************************************************************/

            public void resume ( )
            {
                // Tell Consumes to resume.
                foreach ( subscriber_ids; this.request_ids )
                {
                    foreach ( ids; subscriber_ids )
                    {
                        foreach ( id; ids[] )
                            this.outer.neo_client.control(id,
                                ( RawClient.Neo.Consume.IController controller )
                                {
                                    controller.resume();
                                }
                            );
                    }
                }

                // Wait until they are all suspended.
                uint resumed;
                do
                {
                    this.errors = this.errors.init;
                    this.waiting = true;
                    this.task = Task.getThis();
                    this.task.suspend();
                    this.waiting = false;

                    enforce(!this.errors.stopped,
                        "Stop during Consume request");
                    enforce(!this.errors.channel_removed,
                        "Channel removed during Consume request");
                    enforce(!this.errors.disconnection,
                        "Disconnection during Consume request");
                    enforce(!this.errors.node_error,
                        "Node error during Consume request");

                    resumed += this.errors.resumed;
                }
                while ( resumed < this.request_ids.length );
            }

            /*******************************************************************

                Callback used internally to process consumer events

                Params:
                    info = information on the notification
                    args = arguments used to initiate the request

            *******************************************************************/

            private void notifier ( RawClient.Neo.Consume.Notification info,
                RawClient.Neo.Consume.Args args )
            {
                with ( info.Active ) switch ( info.active )
                {
                    case started:
                        break;

                    case received:
                        this.received_records ~= ReceivedRecord(
                            idup(args.subscriber),
                            idup(args.channel),
                            idup(cast(cstring)info.received.value)
                        );
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case stopped:
                        this.errors.stopped = true;
                        // Use cast(istring) to work around DMD bug 6722.
                        auto ichn = cast(istring)args.channel;
                        auto isub = cast(istring)args.subscriber;
                        if (!this.request_ids[ichn][isub].remove())
                        {
                            this.request_ids[ichn].remove(isub);
                            if (!this.request_ids[ichn].length)
                                this.request_ids.remove(ichn);
                        }
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case suspended:
                        this.errors.suspended++;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case resumed:
                        this.errors.resumed++;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case channel_removed:
                        this.errors.channel_removed = true;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case node_disconnected:
                        log.error(
                            "Consume failed due to connection error {} on {}:{}",
                            getMsg(info.node_disconnected.e),
                            info.node_disconnected.node_addr.address_bytes,
                            info.node_disconnected.node_addr.port);
                        this.errors.disconnection = true;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case node_error:
                        log.error(
                            "Consume failed due to a node error on {}:{}",
                            info.node_error.node_addr.address_bytes,
                            info.node_error.node_addr.port);
                        this.errors.node_error = true;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    case unsupported:
                        log.error(
                            "Consume failed due to an unsupported error on {}:{}",
                            info.unsupported.node_addr.address_bytes,
                            info.unsupported.node_addr.port);
                        this.errors.node_error = true;
                        if (this.waiting)
                            this.task.resume();
                        break;

                    default: assert(false);
                }
            }
        }

        /***********************************************************************

            Convenience getter for the neo object of the swarm client owned by
            the outer class.

        ***********************************************************************/

        private RawClient.Neo neo_client ( )
        {
            return this.outer.raw_client.neo;
        }
    }

    /***************************************************************************

        Wrapper object containing all neo requests.

    ***************************************************************************/

    public Neo neo;

    /***************************************************************************

        Constructor

    ***************************************************************************/

    this ()
    {
        this.log = Log.lookup("dmqtest");
        this.neo = new Neo;

        cstring auth_name = "test";
        auto auth_key = Key.init;
        // At least 3 connections are required, as some tests Consume from two
        // channels in parallel while assigning PushMulti requests.
        auto connections = 3;
        this.raw_client = new RawClient(theScheduler.epoll, auth_name,
            auth_key.content,
            &this.neo.connectionNotifier, connections);
        this.raw_client.neo.enableSocketNoDelay();
    }

    /***************************************************************************

        Helper that wraps DMQ client push request

    ***************************************************************************/

    public void push ( cstring channel, cstring data )
    {
        scope tbr = new TaskBlockingRequest;

        cstring input ( RawClient.RequestContext context )
        {
            return data;
        }

        this.raw_client.assign(
            this.raw_client.push(channel, &input, &tbr.notify)
        );

        tbr.wait();
    }

    /***************************************************************************

        Helper that wraps DMQ client PushMulti request

    ***************************************************************************/

    public void pushMulti ( in cstring[] channels_, cstring data )
    {
        // raw_client.pushMulti() needs cstring[] -- const(char)[][], but the
        // argument is const(char[])[] to allow for calling with istring[].
        // These types are not compatible so duplicate all strings. This should
        // be removed when raw_client.pushMulti() is fixed to accept
        // const(char[])[] or `in cstring[]` for the channels.
        scope tbr = new TaskBlockingRequest;
        auto channels = new cstring[channels_.length];
        foreach (i, ref channel; channels)
            channel = idup(channels_[i]);

        cstring input ( RawClient.RequestContext context )
        {
            return data;
        }

        this.raw_client.assign(
            this.raw_client.pushMulti(channels, &input, &tbr.notify)
        );

        tbr.wait();
    }

    /***************************************************************************

        Helper that wraps DMQ client pop request

    ***************************************************************************/

    public mstring pop ( cstring channel )
    {
        scope tbr = new TaskBlockingRequest;
        mstring result;

        void output ( RawClient.RequestContext context, in cstring value )
        {
            if (value.length)
                result.copy(value);
        }

        this.raw_client.assign(
            this.raw_client.pop(channel, &output, &tbr.notify)
        );

        tbr.wait();
        return result;
    }

    /***************************************************************************

        Get the number of records and bytes in the specified channel.

        Params:
            channel = name of DMQ channel.
            records = receives the number of records in the channel
            bytes = receives the number of bytes in the channel

        Throws:
            upon request error (Exception.msg set to indicate error)

    ***************************************************************************/

    public void getChannelSize ( cstring channel, out ulong records,
        out ulong bytes )
    {
        scope tbr = new TaskBlockingRequest;

        void output ( RawClient.RequestContext context, in cstring address,
            ushort port, in cstring channel, ulong r, ulong b )
        {
            records += r;
            bytes += b;
        }

        this.raw_client.assign(
            this.raw_client.getChannelSize(channel, &output, &tbr.notify)
        );

        tbr.wait();
    }

    /***************************************************************************

        Helper that wraps DMQ client removeChannel request

    ***************************************************************************/

    public void removeChannel ( cstring channel )
    {
        scope tbr = new TaskBlockingRequest;

        this.raw_client.assign(
            this.raw_client.removeChannel(channel, &tbr.notify)
        );

        tbr.wait();
    }

    /***************************************************************************

        Assigns a Produce request and waits until it is ready to accept data.

        Note that, due to its internal buffering, to ensure that all sent
        records are actually received by the DMQ, you need to call the
        producer's finish() method when your test is done sending data.

        Params:
            channel = channel to produce to

        Returns:
            Producer object which can be used to stream data to a DMQ channel

    ***************************************************************************/

    public Producer startProduce ( cstring channel )
    {
        auto producer = new Producer();

        this.raw_client.assign(this.raw_client.produce(channel,
            &producer.producer, &producer.notifier));

        producer.waitNextEvent();

        return producer;
    }

    /***************************************************************************

        Assigns a ProduceMulti request and waits until it is ready to accept
        data.

        Note that, due to its internal buffering, to ensure that all sent
        records are actually received by the DMQ, you need to call the
        producer's finish() method when your test is done sending data.

        Params:
            channels = channels to produce to

        Returns:
            Producer object which can be used to stream data to the specified
            DMQ channels

    ***************************************************************************/

    public Producer startProduceMulti ( in cstring[] channels_ )
    {
        // raw_client.produceMulti() needs cstring[] -- const(char)[][], but
        // the argument is const(char[])[] to allow for calling with istring[].
        // These types are not compatible so duplicate all strings. This should
        // be removed when raw_client.produceMulti() is fixed to accept
        // const(char[])[] or `in cstring[]` for the channels.
        auto channels = new cstring[channels_.length];
        foreach (i, ref channel; channels)
            channel = idup(channels_[i]);

        auto producer = new Producer();

        this.raw_client.assign(this.raw_client.produceMulti(channels,
            &producer.producer, &producer.notifier));

        producer.waitNextEvent();

        return producer;
    }

    /***************************************************************************

        Blocking wrapper on top of Produce request

    ***************************************************************************/

    public static class Producer
    {
        /***********************************************************************

            Flag that indicates that producer was terminated, usually because
            there is no more channel to write to.

        ***********************************************************************/

        public bool finished = false;

        /*******************************************************************

            Set to true when this.task was suspended by waitNextEvent().
            Used to decide whether to resume the task when an event occurs.
            (This instance is not necessarily the only thing controlling
            the task, so it must be sure to only resume the task when it
            was the one who suspended it originally.

        *******************************************************************/

        private bool waiting;

        /***********************************************************************

            Task that's suspended and waiting for the next event.

        ***********************************************************************/

        private Task task;

        /***********************************************************************

            Set of waiting producers.

        ***********************************************************************/

        private RawClient.IProducer[] producers;

        /***********************************************************************

            Suspends the bound task until either the producer is ready to send
            another record or another producer event happens (i.e. termination).

        ***********************************************************************/

        public void waitNextEvent ( )
        {
            if (!this.finished)
            {
                this.waiting = true;
                this.task = Task.getThis();
                this.task.suspend();
                this.waiting = false;
            }
        }

        /***********************************************************************

            Returns:
                true if the producer is ready to send another record

        ***********************************************************************/

        public bool ready_to_send ( )
        {
            return this.producers.length > 0;
        }

        /***********************************************************************

            Write a record to the producer. Should only be called when
            this.ready_to_send is true.

            Params:
                record = record value to send

        ***********************************************************************/

        public void write ( cstring record )
        {
            assert(this.ready_to_send);

            auto producer = this.producers[$-1];
            this.producers.length = this.producers.length - 1;
            enableStomping(this.producers);

            producer(record);

            this.waitNextEvent();
        }

        /***********************************************************************

            Ends the Produce request, flushing all pending records to the DMQ.
            Should only be called when this.ready_to_send is true.

        ***********************************************************************/

        public void finish ( )
        {
            this.write("");
            enforce(this.finished);
        }

        /***********************************************************************

            Callback used internally to receive the IProducer interface

        ***********************************************************************/

        private void producer ( RawClient.RequestContext,
            RawClient.IProducer producer )
        {
            this.producers ~= producer;
            if (this.waiting)
                this.task.resume();
        }

        /***********************************************************************

            Callback used internally to process producer events

        ***********************************************************************/

        private void notifier ( RawClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished )
            {
                this.finished = true;
                if (this.waiting)
                    this.task.resume();
            }
        }
    }

    /***************************************************************************

        Assigns a Consume request and waits until it starts being handled.

        Records received by the consumer are appended to its own internal `data`
        array, which should be managed (iterated, cleared, etc) by the user as
        desired.

        To check that the Consume request has started being handled, a dummy
        record is written to the DMQ channel and the fiber suspended until some
        data is read from the channel. This may either be the dummy record (in
        the case where the channel is otherwise empty) or may be a real record
        from the channel (if the channel was non-empty). In either case, the
        dummy record, when it's received by the consumer, is *not* added to the
        consumer's `data` array.

        Params:
            channel = channel to consume

    ***************************************************************************/

    public Consumer startConsume ( cstring channel )
    {
        auto consumer = new Consumer();

        this.raw_client.assign(this.raw_client.consume(channel,
            &consumer.record, &consumer.notifier));

        this.push(channel, Consumer.marker_value);

        if (!consumer.data.length)
            consumer.waitNextEvent();
        enforce(!consumer.finished);

        return consumer;
    }

    /***************************************************************************

        Blocking wrapper on top of Consume request

    ***************************************************************************/

    public static class Consumer
    {
        /***********************************************************************

            Dummy value pushed to DMQ before consuming so that one record will
            always arrive immediately even if DMQ was empty at that point.

        ***********************************************************************/

        static istring marker_value =
            "dmqtest marker value to ensure consume has started";

        /***********************************************************************

            Flag that indicates that consumer was terminated, usually because
            there is no more channel to consume.

        ***********************************************************************/

        public bool finished = false;

        /*******************************************************************

            Task that gets suspended when `waitNextEvent` is called.

        *******************************************************************/

        private Task task;

        /*******************************************************************

            Set tu true when this.task was suspended by waitNextEvent().
            Used to decide whether to resume the task when an event occurs.
            (This instance is not necessarily the only thing controlling
            the task, so it must be sure to only resume the task when it
            was the one who suspended it originally.

        *******************************************************************/

        private bool waiting;

        /*******************************************************************

            Indicator if the new event has already happened before the task
            is about to be suspended, so it can continue without
            suspending.

        *******************************************************************/

        private bool signaled;

        /***********************************************************************

            New consumed records will be appended to this array (in the end)

        ***********************************************************************/

        public cstring[] data;

        /*******************************************************************

            Returns when either data has been received by the Consume request
            or the channel being consumed has been removed. If neither of
            those things are immediately true, when the method is called,
            the bound task is suspended until one of them occurs. Thus, when
            this method returns, one of the following has happened:

                1. Data was already available (in this.data), so the task
                   was not suspended.
                2. The Consume request has finished, so the task was not
                   suspended.
                3. The task was suspended, new data arrived, and the task
                   was resumed. The new data is added to this.data where it
                   can be read by the test case. When the test case has finished
                   checking the received data, it must remove it from the AA,
                   otherwise subsequent calls to waitNextEvent() will
                   always return immediately (case 1), without waiting.
                4. The task was suspended, the Consume request terminated due
                   to the channel being removed, and the task was resumed.
                   It is not possible to make further use of this instance.

        *******************************************************************/

        public void waitNextEvent ( )
        {
            if (this.signaled)
            {
                this.signaled = false;
                return;
            }

            this.waiting = true;
            this.task = Task.getThis();
            this.task.suspend();
            this.waiting = false;
        }

        /***********************************************************************

            Callback used internally to store consumed records

        ***********************************************************************/

        private void record ( RawClient.RequestContext, in cstring data )
        {
            // Ignore empty end marker - the end of consume
            // will be signaled via notifier
            // TODO: this bug is fixed in v10.1.1 so this can be removed
            // upon merging that into v12.x.x
            if (!data.length)
                return;

            if (data != marker_value)
                this.data ~= data.dup;

            if (this.waiting)
            {
                this.task.resume();
            }
            else
            {
                this.signaled = true;
            }
        }

        /***********************************************************************

            Callback used internally to process consumer events

        ***********************************************************************/

        private void notifier ( RawClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished )
            {
                this.finished = true;

                if (this.waiting)
                {
                    this.task.resume();
                }
                else
                {
                    this.signaled = true;
                }
            }
        }
    }


    /**************************************************************************

        Forward to standard DmqClient's `addNode`

    **************************************************************************/

    public void addNode(cstring addr, ushort port)
    {
        this.raw_client.addNode(addr.dup, port);
        ushort neo_port = port;
        neo_port++;
        this.raw_client.neo.addNode(addr, neo_port);
    }
}
