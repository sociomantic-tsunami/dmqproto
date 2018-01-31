/*******************************************************************************

    Fake DMQ node Consume request implementation.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.neo.request.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import dmqproto.node.neo.request.Consume;

import fakedmq.Storage;

import ocean.transition;

/*******************************************************************************

    Fake node implementation of the v3 Consume request protocol.

*******************************************************************************/

class ConsumeImpl_v3 : ConsumeProtocol_v3, DmqListener
{
    /***************************************************************************

        Remember consumed channel

    ***************************************************************************/

    private Queue queue;

    /***************************************************************************

        Performs any logic needed to start consuming from the channel of the
        given name.

        Params:
            channel_name = channel to subscribe to
            subscriber_name = subscriber name (v2 only)

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name,
        cstring subscriber_name )
    {
        this.queue = global_storage.getCreate(channel_name)
            .subscribe(subscriber_name);
        this.queue.register(this);
        return true;
    }

    /***************************************************************************

        Performs any logic needed to stop consuming from the channel of the
        given name.

        Params:
            channel_name = channel to stop consuming from

    ***************************************************************************/

    override protected void stopConsumingChannel ( cstring channel_name )
    {
        this.queue.unregister(this);
    }

    /***************************************************************************

        Retrieve the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    override protected bool getNextValue ( ref void[] value )
    {
        size_t records, bytes;
        this.queue.countSize(records, bytes);

        if ( records == 0 )
        {
            // We've flushed all data
            this.queue.consumerFlushed();
            return false;
        }

        value = this.queue.pop().dup;
        return true;
    }

    /***************************************************************************

        DmqListener interface method. Called by Storage when new data arrives
        or the channel is deleted.

        Params:
            code = trigger event code

    ***************************************************************************/

    override public void trigger ( Code code )
    {
        with ( Code ) switch ( code )
        {
            case DataReady:
                this.dataReady();
                break;

            case Flush:
                this.flushBatch();
                break;

            case Finish:
                this.channelRemoved();
                break;
            default:
                break;
        }
    }
}
