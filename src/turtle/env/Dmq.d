/*******************************************************************************

    DMQ Node Library Implementation

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module turtle.env.Dmq;

import ocean.transition;

import turtle.env.model.Node;

import fakedmq.DmqNode;
import fakedmq.Storage;

import ocean.core.Test;
import ocean.task.util.Timer;
import ocean.task.Scheduler;

/*******************************************************************************

    Aliases to exceptions thrown on illegal operations with dmq storage

    Check `Throws` DDOC sections of methods in this module to see when
    exactly these can be thrown.

*******************************************************************************/

public alias fakedmq.Storage.MissingChannelException MissingChannelException;
public alias fakedmq.Storage.EmptyChannelException EmptyChannelException;
public alias fakedmq.Storage.FullChannelException FullChannelException;

/*******************************************************************************

    Returns:
        singleton DMQ instance to be used from tests.

*******************************************************************************/

public Dmq dmq()
in
{
    assert (_dmq !is null, "Must call `Dmq.initialize` first");
}
body
{
    return _dmq;
}

private Dmq _dmq;

/*******************************************************************************

    The Dmq class wraps access to fake turtle DMQ node implementation. Only
    one Dmq object is allowed to exist through the program runtime.

*******************************************************************************/

public class Dmq : Node!(DmqNode, "dmq")
{
    import dmqproto.client.legacy.DmqConst;

    import ocean.core.Enforce;
    import ocean.text.convert.Formatter;
    import ocean.util.serialize.contiguous.package_;
    import swarm.neo.AddrPort;
    import swarm.Const: NodeItem;

    import core.sys.posix.netinet.in_: AF_INET,  INET_ADDRSTRLEN;
    import core.sys.posix.arpa.inet: socklen_t, inet_ntop;
    import core.stdc.string: strlen;

    /***************************************************************************

        Prepares DMQ singleton for usage from tests

    ***************************************************************************/

    public static void initialize ( )
    {
        if ( !_dmq )
            _dmq = new Dmq();
    }

    /***************************************************************************

        Pushes a data item to a queue channel. If channel is full, waits until
        it gets consumed enough to push new record.

        Note: If the data type is not a char[]/void[] it will be serialized before
        being written to the queue.

        Params:
            channel = name of the queue channel to which data should be pushed.
            data = data item to be pushed to the queue channel.

    ***************************************************************************/

    public void push ( T ) ( cstring channel, T data )
    {
        // make the function work with static arrays
        static if (is(T : cstring) || is(T : Const!(void)[]))
        {
            auto serialized_data = cast(mstring) data.dup;
        }
        else
        {
            void[] buf;
            auto serialized_data =
                cast(mstring) Serializer.serialize(data, buf);
        }

        enforce(serialized_data.length,
            "Cannot push empty data to the queue!");

        for (;;)
        {
            try
            {
                global_storage.getCreate(channel).push(serialized_data);
                return;
            }
            catch (FullChannelException)
            {
                .wait(50_000);
            }
        }
    }

    unittest
    {
        struct S { int x; }

        // ensures compilation
        void stub ( )
        {
            dmq.push("abc", S.init);
            dmq.push("abc", "data");
        }
    }

    /***************************************************************************

        Pop the next item from the specified queue channel and return it.

        Params:
            channel = name of queue channel from which an item should be popped.

        Returns:
            The popped data.

        Throws:
            MissingChannelException if channel does not exist
            EmptyChannelException if channel has no more records

    ***************************************************************************/

    public T pop ( T = mstring ) ( cstring channel )
    {
        auto queue = global_storage.getVerify(channel).queue_unless_subscribed;

        if (queue is null)
            throw new EmptyChannelException;

        auto result = queue.pop();

        static if (is(T : cstring) || is(T : Const!(void)[]))
        {
            return cast(T) result.dup;
        }
        else
        {
            Contiguous!(T) buf;
            return *Deserializer.deserialize!(T)(cast(void[]) result, buf).ptr;
        }
    }

    /***************************************************************************

        Sets global queue channel size limit

        Params:
            new_size = new channel size limit

    ***************************************************************************/

    public void maxChannelSize ( size_t new_size )
    {
        global_storage.channel_size_limit = new_size;
    }

    /***************************************************************************

        Packs together channel size and length data

    ***************************************************************************/

    struct ChannelSize
    {
        size_t records, bytes;
    }

    /***************************************************************************

        Gets the size of the specified queue channel (in number of records and
        in bytes) and returns it

        Params:
            channel = name of queue channel to get size of

        Returns:
            Size of specified channel

    ***************************************************************************/

    public ChannelSize getSize ( cstring channel )
    {
        auto channel_obj = global_storage.get(channel);
        if (channel_obj is null)
            return ChannelSize.init;

        ChannelSize result, single;

        foreach (queue; global_storage.get(channel))
        {
            queue.countSize(single.records, single.bytes);
            result.records += single.records;
            result.bytes += single.bytes;
        }

        return result;
    }

    // instantiate templated methods to make sure they're semantically analyzed
    version (UnitTest)
    {
        private struct _Struct
        {
            int x;
            float y;
        }

        private alias pop!(mstring) _popCharArr;
        private alias pop!(_Struct) _popStruct;

        private alias push!(char[2]) _pushCharStatArr;
        private alias push!(mstring) _pushCharArr;
        private alias push!(_Struct) _pushStruct;
    }

    /***************************************************************************

        Waits until at least `count` records can be found in specified DMQ
        channel or until timeout is hit.

        Params:
            op = the condition to use, e.g. '==' if an exact match is required
                 set to '>=' by default
            channel = DMQ channel to check
            count = expected amount of records
            timeout = max time allowed to wait
            check_interval = time between polling channel state

        Throws:
            TestException if timeout has been hit

    ***************************************************************************/

    public void waitTotalRecords ( istring op = ">=" ) ( cstring channel,
        size_t count, double timeout = 1.0, double check_interval = 0.05 )
    {
        size_t recordCount ( cstring channel )
        {
            return this.getSize(channel).records;
        }

        auto total_wait = 0.0;

        do
        {
            if (mixin("recordCount(channel)" ~ op ~ "count"))
                return;
            .wait(cast(uint) (check_interval * 1_000_000));
            total_wait += check_interval;
        }
        while (total_wait < timeout);

        throw new TestException(format(
            "Expected {} records in channel '{}', got only {} during {} seconds",
            count,
            channel,
            recordCount(channel),
            timeout
        ));
    }

    /***************************************************************************

        Creates a fake node at the specified address/port.

        Params:
            node_addrport = address/port

    ***************************************************************************/

    override public DmqNode createNode ( AddrPort node_addrport )
    {
        auto node_item = NodeItem(new char[INET_ADDRSTRLEN], node_addrport.port());
        Const!(char*) addrp = inet_ntop(AF_INET, &node_addrport.naddress,
            node_item.Address.ptr, cast(socklen_t)node_item.Address.length);
        assert(addrp);
        node_item.Address = node_item.Address.ptr[0 .. strlen(node_item.Address.ptr)];

        auto epoll = theScheduler.epoll();

        auto node = new DmqNode(node_item, epoll);
        node.register(epoll);
        theScheduler.processEvents();

        return node;
    }

    /***************************************************************************

        Returns:
            address/port on which node is listening

    ***************************************************************************/

    override public AddrPort node_addrport ( )
    {
        AddrPort addrport;
        addrport.setAddress(this.node.node_item.Address);
        addrport.port = this.node.node_item.Port;

        return addrport;
    }

    /***************************************************************************

        Stops the fake DMQ service. The node may be started again on the same
        port via restart().

    ***************************************************************************/

    override protected void stopImpl ( )
    {
        this.node.stopListener(theScheduler.epoll);
        this.node.shutdown();
    }

    /***************************************************************************

        Removes all data from the fake node service.

    ***************************************************************************/

    override public void clear ( )
    {
        global_storage.clear();
    }

    /***************************************************************************

        Removes all channels and terminates all active Consume requests.

        This method needs to be called instead of `clear` if application
        is restarted between tests to ensure no requests remain hanging. In all
        other cases prefer `clear`.

    ***************************************************************************/

    override public void reset ( )
    {
        foreach (channel; global_storage.getChannelList())
            global_storage.remove(channel);
    }

    /***************************************************************************

        Suppresses/allows log output from the fake node if used version of node
        proto supports it.

        Params:
            log = true to log errors, false to stop logging errors

    ***************************************************************************/

    override public void log_errors ( bool log )
    {
        this.node.log_errors = log;
    }

    /***************************************************************************

        Modifies internal channel size limit in backing storage. Won't cause
        data to be reallocated or shrunk, only affects if new records are
        rejected or not.

        Params:
            limit = new limit size

    ***************************************************************************/

    public void setSizeLimit ( size_t limit )
    {
        global_storage.channel_size_limit = limit;
    }

}

version (UnitTest)
{
    import ocean.core.Test;

    void initDmq ( )
    {
        global_storage.clear();
        Dmq.initialize();
    }
}

/*******************************************************************************

    getSize() tests

*******************************************************************************/

unittest
{
    // Empty channel
    {
        initDmq();
        auto size = dmq.getSize("non_existent_channel");
        test!("==")(size.records, 0);
        test!("==")(size.bytes, 0);
    }

    // Channel with one record
    {
        initDmq();
        dmq.push("unittest_channel", "abcd"[]);
        auto size = dmq.getSize("unittest_channel");
        test!("==")(size.records, 1);
        test!("==")(size.bytes, 4);
    }
}
