/*******************************************************************************

    Local store used to verify the results of operations on the DMQ being tested.

    The local store contains a map of queues, indexed by channel name. When used
    in tests, the queues should be updated in the same way as the DMQ channels
    being tested (e.g. when a record is pushed to the DMQ, the same record
    should be pushed to the local queue). The verifyAgainstDmq() method then
    performs a series of tests to confirm that the content of the DMQ matches
    the content of the local store, as far as is possible.

    Copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.util.LocalStore;

struct LocalStore
{
    import dmqtest.DmqClient;
    import turtle.runner.Logging;
    import ocean.core.array.Search : contains;
    import ocean.core.Test;
    import ocean.transition;

    /***************************************************************************

        Queue of records in a single channel.

    ***************************************************************************/

    private static struct RecordQueue
    {
        import ocean.core.Array : removeShift;

        /***********************************************************************

            Queue of records.

        ***********************************************************************/

        private mstring[] queue;

        /***********************************************************************

            Push the specified record to the queue.

            Params:
                record = record to push

        ***********************************************************************/

        public void push ( cstring record )
        {
            this.queue ~= record.dup;
        }

        /***********************************************************************

            Pop a record from the queue.

            Returns:
                popped record or null if the queue is empty

        ***********************************************************************/

        public cstring pop ( )
        {
            if ( !this.queue.length )
                return null;

            auto record = this.queue[0];
            this.queue.removeShift(0);
            return record;
        }

        /***********************************************************************

            Returns:
                the number of records in the queue

        ***********************************************************************/

        public size_t length ( )
        {
            return this.queue.length;
        }
    }

    /***************************************************************************

        Per-channel queue of data in local store

    ***************************************************************************/

    private RecordQueue[cstring] data;

    /***************************************************************************

        Returns:
            the names of the channels which exist in the local store

    ***************************************************************************/

    public cstring[] channels ( )
    {
        return this.data.keys;
    }

    /***************************************************************************

        Pushes a record to the local store.

        Params:
            channel = channel to push to
            val = record value

    ***************************************************************************/

    public void push ( cstring channel, cstring record )
    {
        if ( !(channel in this.data) )
            this.data[channel] = RecordQueue();
        this.data[channel].push(record);
    }

    /***************************************************************************

        Pops a record from the local store.

        Params:
            channel = channel to pop from

        Returns:
            popped record or null if the local store is empty

    ***************************************************************************/

    public cstring pop ( cstring channel )
    {
        if ( !(channel in this.data) )
            return null;

        return this.data[channel].pop();
    }
}

