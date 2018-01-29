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

        Constructor

    ***************************************************************************/

    this ()
    {
        this.log = Log.lookup("dmqtest");

        // At least 3 connections are required, as some tests Consume from two
        // channels in parallel while assigning PushMulti requests.
        auto connections = 3;
        this.raw_client = new RawClient(theScheduler.epoll, connections);
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
    }
}
