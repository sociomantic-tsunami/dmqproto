/*******************************************************************************

    Implements very simple queue storage based on built-in arrays. Simplicity
    is the primary goal here as performance does not matter much for turtle
    tests.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.Storage;

/******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Enforce;
import ocean.task.Task;
import ocean.task.Scheduler;

import swarm.node.storage.listeners.Listeners;

/*******************************************************************************

    Dmq listener interface type

*******************************************************************************/

public alias IListenerTemplate!() DmqListener;

/*******************************************************************************

    Global data object to be used from tests and request handlers

*******************************************************************************/

public Storage global_storage;

/*******************************************************************************

    Wraps associative array of channels to add some convenience methods

*******************************************************************************/

public struct Storage
{
    /***************************************************************************

        Backing storage.

    ***************************************************************************/

    private Channel[cstring] channels;

    /***************************************************************************

        Artifical size limit for queue channels. While the DMQ protocol does not
        require channels to have a maximum size, it is implemented in the fake
        DMQ node in order to provide an early warning, before hitting an OS out
        of memory situation.

    ***************************************************************************/

    public size_t channel_size_limit = 1024 * 1024;

    /***************************************************************************

        Count total size taken by storage

        Params:
            records = total amount of records
            bytes   = total size of all records

    ***************************************************************************/

    public void countSize ( out size_t records, out size_t bytes )
    {
        size_t channel_records, channel_bytes;

        foreach (name, channel; this.channels)
        {
            foreach (queue; channel)
                queue.countSize(channel_records, channel_bytes);
            records += channel_records;
            bytes += channel_bytes;
        }
    }

    /***************************************************************************

        Returns:
            all stored channels as a name array

    ***************************************************************************/

    public cstring[] getChannelList ( )
    {
        cstring[] list;
        foreach (channel, data; this.channels)
            list ~= channel;
        return list;
    }

    /***************************************************************************

        Looks for specified channel.

        Params:
            name = channel name
    
        Returns:
            specified channel, null if it doesn't exist

    ***************************************************************************/

    public Channel get(cstring name)
    {
        auto channel = name in this.channels;
        return channel is null ? null : *channel;
    }

    /***************************************************************************

        Looks for specified channel and throws if missing

        Params:
            name = channel name
    
        Returns:
            specified channel

        Throws:
            MissingChannelException if missing

    ***************************************************************************/

    public Channel getVerify(cstring name)
    {
        auto channel = name in this.channels;
        enforce!(MissingChannelException)(channel !is null);
        return *channel;
    }

    /***************************************************************************

        Looks for specified channel and creates one if missing

        Params:
            name = channel name
    
        Returns:
            specified channel

    ***************************************************************************/

    public Channel getCreate(cstring name)
    {
        auto channel = name in this.channels;
        if (channel is null)
        {
            this.channels[idup(name)] = new Channel;
            channel = name in this.channels;
        }
        return *channel;
    }

    /***************************************************************************
    
        Removes specified channel from the storage

        Params:
            name = channel name

    ***************************************************************************/

    public void remove(cstring name)
    {
        auto channel = name in this.channels;
        if (channel is null)
            return;

        foreach (queue; *channel)
            queue.consumers.trigger(IListener.Code.Finish);

        this.channels.remove(name);
    }

    /***************************************************************************

        Empties all channels in the storage

    ***************************************************************************/

    public void clear ( )
    {
        auto names = this.channels.keys;
        foreach (name; names)
        {
            foreach (queue; this.getVerify(name))
                queue.queue = null;
        }
    }

    /***************************************************************************

        Flushes all consumers.

    ***************************************************************************/

    public void flushAllConsumers ( )
    {
        foreach (channel; this.channels)
        {
            foreach (queue; channel)
                queue.consumers.trigger(DmqListener.Code.Flush);
        }
    }

    /***************************************************************************

        Removes all data about registered consumers from channels

        Intended as a tool for clean restart, must not be called while node
        is active and serving requests.

    ***************************************************************************/

    public void dropAllConsumers ( )
    {
        foreach (channel; this.channels)
        {
            foreach (queue; channel)
                queue.consumers = queue.new Consumers;
        }
    }
}

/*******************************************************************************

    Manages channel subscribers.

*******************************************************************************/

class Channel
{
    /***************************************************************************

        The initial queue until a subscriber is added.

    ***************************************************************************/

    private Queue init_queue;

    /***************************************************************************

        The subscriber queues by subscriber name after a subscriber has been
        added.

    ***************************************************************************/

    private Queue[istring] subscribers;

    /***************************************************************************

        Make sure either `init_queue` or `subscribers` is active.

    ***************************************************************************/

    invariant ( )
    {
        if (this.init_queue is null)
        {
            assert(this.subscribers.length);
        }
        else
        {
            assert(!this.subscribers.length);
        }
    }

    /***************************************************************************

        Constructor, creates the initial queue.

    ***************************************************************************/

    public this ( )
    {
        this.init_queue = new Queue;
    }

    /***************************************************************************

        Pushes `value` to all queues in this channel.
        If called inside a task context, will suspend calling task until
        registered DMQ listeners will receive the value that was pushed.

        Params:
            value = record to add to the queue

    ***************************************************************************/

    public void push ( in Queue.ValueType value )
    {
        if (this.init_queue !is null)
            this.init_queue.push(value);
        else
        {
            foreach (queue; this.subscribers)
                queue.push(value);
        }
    }

    /***************************************************************************

        Looks up the queue for `subscriber_name` or adds it if not found.

        Returns:
            the queue for `subscriber_name`.

    ***************************************************************************/

    public Queue subscribe ( cstring subscriber_name )
    {
        if (auto subscriber = subscriber_name in this.subscribers)
        {
            return *subscriber;
        }
        else if (this.init_queue is null)
        {
            return this.subscribers[idup(subscriber_name)] = new Queue;
        }
        else
        {
            scope (exit) this.init_queue = null;
            return this.subscribers[idup(subscriber_name)] = this.init_queue;
        }
    }

    /***************************************************************************

        Returns the initial queue if there are no subscribers.

        Returns:
            the initial queue or `null` if there are subscribers.

    ***************************************************************************/

    public Queue queue_unless_subscribed ( )
    {
        return this.init_queue;
    }

    /***************************************************************************

        `foreach` iteration over the queues in this channel.

    ***************************************************************************/

    public int opApply ( scope int delegate ( ref Queue queue ) dg )
    {
        if (this.init_queue !is null)
            return dg(this.init_queue);
        else
        {
            foreach (ref queue; this.subscribers)
            {
                if (int x = dg(queue))
                    return x;
            }

            return 0;
        }
    }
}

/*******************************************************************************

    Wraps array of values (queue entries) to add some convenience methods

*******************************************************************************/

class Queue
{
    /***************************************************************************

        DMQ record type.

    ***************************************************************************/

    public alias void[] ValueType;

    /***************************************************************************

        DMQ listeners consume the data so only one needs to be notified when
        new data arrives.

    ***************************************************************************/

    private class Consumers : IListeners!()
    {
        /***********************************************************************

            Number of consumers currently writing

        ***********************************************************************/

        private size_t sending_consumers;

        /***********************************************************************

            Caller task to resume when output is flushed

        ***********************************************************************/

        private Task caller;

        /***********************************************************************

            Suspends the caller task until all consumers which are currently
            sending data to the client are done (i.e. are back in the state of
            waiting for more data).

        ***********************************************************************/

        public void waitUntilFlushed ( )
        {
            if ( !this.sending_consumers )
                return;

            enforce(this.caller is null);
            this.caller = Task.getThis();
            enforce(this.caller !is null);

            this.caller.suspend();
            this.caller = null;
        }

        /***********************************************************************

            Indicates that a consumer has finished sending data to the client.

            If no registered consumers are sending currently, does nothing.

            If one or more registered consumers are sending, when all consumers
            have finished sending data, if a task was previously registered
            via waitUntilFlushed(), it is resumed.

        ***********************************************************************/

        public void consumerFlushed ( )
        {
            if ( !this.sending_consumers )
                return;

            this.sending_consumers--;

            if ( this.sending_consumers == 0 && (this.caller !is null) )
            {
                this.caller.resume();
            }
        }

        /***********************************************************************

            Triggers a single consumer and tracks the number of consumers in the
            sending state.

            Params:
                code = code of triggered event

        ***********************************************************************/

        override protected void trigger_ ( DmqListener.Code code )
        {
            switch (code)
            {
                case code.DataReady:
                    auto listener = this.listeners.next();
                    if (listener)
                    {
                        this.sending_consumers++;
                        listener.trigger(code);
                    }
                    break;
                case code.Flush:
                    super.trigger_(code);
                    break;
                case code.Finish:
                    this.sending_consumers += this.listeners.length;
                    super.trigger_(code);
                    break;
                default:
                    assert(false);
            }
        }
    }

    /***************************************************************************

        Request contexts waiting for more data in this channel

    ***************************************************************************/

    private Consumers consumers;

    /***************************************************************************

        Underlying data storage.

    ***************************************************************************/

    private ValueType[] queue;

    /***************************************************************************

        Constructor

    ***************************************************************************/

    this ( )
    {
        this.consumers = new Consumers;
    }

    /***************************************************************************

        Count total size taken by channel

        Params:
            records = total amount of records
            bytes   = total size of all records

    ***************************************************************************/

    public void countSize ( out size_t records, out size_t bytes )
    {
        records = this.queue.length;
        bytes = 0;
        foreach (value; this.queue)
            bytes += value.length;
    }

    /***************************************************************************

        Returns:
            least recent record added to the queue

    ***************************************************************************/

    public const(ValueType) pop ( )
    {
        enforce!(EmptyChannelException)(this.queue.length != 0);

        auto value = this.queue[$-1];
        this.queue = this.queue[0 .. $ - 1];
        assumeSafeAppend(this.queue);
        return value;
    }

    /***************************************************************************

        If called inside a task context, will suspend calling task until
        registered DMQ listeners will receive the value that was pushed.

        Params:
            value = record to add to the queue

    ***************************************************************************/

    public void push ( in ValueType value )
    {
        size_t records, bytes;
        this.countSize(records, bytes);
        enforce!(FullChannelException)(
            bytes + value.length < global_storage.channel_size_limit);

        this.queue.length = this.queue.length + 1;
        assumeSafeAppend(this.queue);
        for (size_t i = this.queue.length - 1; i > 0; --i)
            this.queue[i] = this.queue[i - 1];
        this.queue[0] = value.dup;

        this.consumers.trigger(IListener.Code.DataReady);
        if (Task.getThis() !is null)
            this.consumers.waitUntilFlushed();
    }

    /***************************************************************************

        If called inside a task context, will suspend calling task until
        registered DMQ listeners will receive values that were pushed.

        Params:
            values = array of records to add to the queue. Added in reverse
                order (first element of array will get popped last)

    ***************************************************************************/

    public void push ( in ValueType[] values )
    {
        size_t records, bytes, values_length;
        foreach (value; values)
            values_length += value.length;
        this.countSize(records, bytes);
        enforce!(FullChannelException)(
            bytes + values_length < global_storage.channel_size_limit);

        this.queue.length = this.queue.length + values.length;
        for (size_t i = this.queue.length - values.length; i >= values.length; --i)
            this.queue[i] = this.queue[i - 1];

        foreach ( i, val; values )
            this.queue[i] = val.dup;

        this.consumers.trigger(IListener.Code.DataReady);
        if (Task.getThis() !is null)
            this.consumers.waitUntilFlushed();
    }

    /***************************************************************************

        Registers a consumer with the channel. The dataReady() method of the
        given consumer may be called when data is put to the channel.

        Params:
            consumer = consumer to notify when data is ready

    ***************************************************************************/

    public void register ( DmqListener consumer )
    {
        this.consumers.register(consumer);
    }

    /***************************************************************************

        Unregisters a consumer from the channel.

        Params:
            consumer = consumer to stop notifying when data is ready

    ***************************************************************************/

    public void unregister ( DmqListener consumer )
    {
        this.consumers.unregister(consumer);
    }

    /***************************************************************************

        Indicates that a consumer has finished sending data to the client.

    ***************************************************************************/

    public void consumerFlushed ( )
    {
        this.consumers.consumerFlushed();
    }

    /***************************************************************************

        Returns:
            the number of consumers which are currently sending data

    ***************************************************************************/

    public size_t sending_consumers ( )
    {
        return this.consumers.sending_consumers;
    }
}

/*******************************************************************************

    Exception that indicates invalid operation with non-existent channel

*******************************************************************************/

class MissingChannelException : Exception
{
    this(cstring name, istring file = __FILE__, int line = __LINE__)
    {
        super("Trying to work with non-existent channel " ~ idup(name), file, line);
    }
}

/*******************************************************************************

    Exception that indicates read attempt from an empty queue channel

*******************************************************************************/

class EmptyChannelException : Exception
{
    this(istring file = __FILE__, int line = __LINE__)
    {
        super("Trying to read from an empty channel", file, line);
    }
}

/*******************************************************************************

    Exception that indicates write attempt to full channel

*******************************************************************************/

class FullChannelException : Exception
{
    this(istring file = __FILE__, int line = __LINE__)
    {
        super("Trying to write to a full channel. If your test really needs to "
            ~ "write this much data to the DMQ, please change the value of "
            ~ "Storage.channel_size_limit.", file, line);
    }
}

version ( unittest )
{
    import ocean.core.Test;
}

/*******************************************************************************

    Test that the enforce in Consumers.waitUntilFlushed() (that the method is
    not already waiting) does not fire.

*******************************************************************************/

unittest
{
    Storage dmq;
    auto channel = dmq.getCreate("test_channel").queue_unless_subscribed;

    // Fake consumer class, required by Channel.register().
    class FakeConsumer : DmqListener
    {
        size_t count;

        override void trigger ( Code )
        {
            channel.consumerFlushed();
            ++count;
        }
    }

    // Register a consumer with a test DMQ channel.
    auto consumer = new FakeConsumer;
    channel.register(consumer);

    // Define a task that tries to push one value as soon as event loop
    // is started
    class TestTask : Task
    {
        bool error;

        override public void run ( )
        {
            theScheduler.processEvents();

            try
            {
                channel.push("key");
            }
            catch ( Exception e )
            {
                error = true;
            }
        }
    }

    initScheduler(SchedulerConfiguration.init);

    auto task1 = new TestTask;
    auto task2 = new TestTask;

    // schedule two identical tasks simultaneously
    theScheduler.schedule(task1);
    theScheduler.schedule(task2);

    theScheduler.eventLoop();

    // ensures two pushing tasks don't interfere with each other
    test(!task1.error);
    test(!task2.error);

    // ensures total amount of records consumed equals to amount pushed
    test(consumer.count == 2);
}
