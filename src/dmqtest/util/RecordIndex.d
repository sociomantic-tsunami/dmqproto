/*******************************************************************************

    Helper class for neo consume checkers to verify that the records received
    for each channel match the records previously sent. It is needed for neo
    consume checkers because they receive the records for all channels at once.

    The record index contains a map of queues, indexed by channel name. When
    used in tests, the queues should be populated with the records pushed to the
    DMQ channels being tested (e.g. the channel names should be passed to the
    `RecordIndex` constructor and the list of records that are pushed to the DMQ
    should be to `fill()`). When receiving a record from the DMQ during the test
    `pop()` can be used to verify that the received record is the record
    previously pushed.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqtest.util.RecordIndex;

class RecordIndex
{
    import ocean.core.Test;
    import ocean.transition;

    /***************************************************************************

        Per-channel queue of records in local store

    ***************************************************************************/

    private Const!(char[])[][Const!(char[])] records_per_channel;

    /***************************************************************************

        Constructor, creates an empty queue per channel.

        Params:
            channels = the names of the channels to use

    ***************************************************************************/

    public this ( Const!(char[])[] channels ... )
    {
        foreach (channel; channels)
            this.records_per_channel[channel] = null;
    }

    /***************************************************************************

        Populates all channels with `records`, replacing any records that are
        already in the channels.

        Stores the `records` array slice so do not modify `records` while using
        this instance or until calling this method again.

        Params:
            records = the records to put in the channels

        Returns:
            the total number of records in all channels

    ***************************************************************************/

    public size_t fill ( Const!(char[])[] records )
    {
        foreach (ref records_in_channel; this.records_per_channel)
            records_in_channel = records;

        return records.length * this.records_per_channel.length;
    }

    /***************************************************************************

        Pops one record from `channel`, expecting the channel to contain a
        record.

        Params:
            channel = the channel to pop from

        Returns:
            the record popped from `channel`.

        Throws:
            TestException if `channel` doesn't match any of the channel names
            passed to the constructor or the channel is empty.

    ***************************************************************************/

    public cstring pop ( cstring channel,
        istring file = __FILE__, int line = __LINE__ )
    {
        auto records_in_channel = channel in this.records_per_channel;
        test!("!is")(records_in_channel, null, file, line);
        test!("!=")(records_in_channel.length, 0, file, line);
        scope (exit)
            *records_in_channel = (*records_in_channel)[1 .. $];
        return (*records_in_channel)[0];
    }

    /***************************************************************************

        `foreach` iteration over the channel names passed to the constructor.

    ***************************************************************************/

    public int opApply ( scope int delegate ( ref cstring channel ) dg )
    {
        foreach (channel, records; this.records_per_channel)
        {
            if (int x = dg(channel))
                return x;
        }
        return 0;
    }

    /***************************************************************************

        Returns:
            the number of channels.

    ***************************************************************************/

    public size_t num_channels ( )
    {
        return this.records_per_channel.length;
    }
}
