/*******************************************************************************

    Consume request class.

    Copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module fakedmq.request.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import Protocol = dmqproto.node.request.Consume;
import fakedmq.Storage;
import ocean.core.TypeConvert;

/*******************************************************************************

    Consume request

*******************************************************************************/

public scope class Consume : Protocol.Consume, DmqListener
{
    import fakedmq.mixins.RequestConstruction;

    /***************************************************************************

        Indicates whether the write buffer should be flushed. In the fake node,
        the buffer is flushed every time one or more records are written in the
        inner while loop in super.handleChannelRequest(). This is to ensure that
        data is forwarded to the client as soon as possible, as test programs
        may not send a continuous stream of Push requests to flush out the
        consumer's write buffer (like a DMQ channel in the live system will
        receive).

    ***************************************************************************/

    private bool need_flush;

    /***************************************************************************

        Indicates that channel has been deleted and request needs to be 
        terminated

    ***************************************************************************/

    private bool channel_deleted;

    /***************************************************************************

        Remember consumed queue

    ***************************************************************************/

    private Queue queue;

    /***************************************************************************

        Provides constructor and common set of private member fields

    ***************************************************************************/

    mixin RequestConstructor!();

    /***************************************************************************

        Initialize the channel iteration

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with request

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.queue = global_storage.getCreate(channel_name).subscribe("");
        return true;
    }

    /***************************************************************************

        Retrieve next value from the channel if available

        Params:
            channel_name = channel to get value from
            value        = array to write value to

        Returns:
            `true` if there was a value in the channel

    ***************************************************************************/

    override protected bool getNextValue ( cstring channel_name, ref mstring value )
    {
        try
        {
            value = castFrom!(void[]).to!(mstring)(this.queue.pop().dup);
            this.need_flush = true;
            return true;
        }
        catch (EmptyChannelException)
        {
            return false;
        }
    }

    /***************************************************************************

        When there are no more elements in the channel this method allows to
        wait for more to appear or force early termination of the request.

        This method is explicitly designed to do a fiber context switch

        Params:
            finish = set to true if request needs to be ended
            flush =  set to true if socket needs to be flushed

    ***************************************************************************/

    override protected void waitEvents ( out bool finish, out bool flush )
    {
        auto id = cast(size_t) cast(void*) this;

        // got deleted while waiting for more data
        if (this.channel_deleted)
        {
            finish = true;
        }
        else if (this.need_flush)
        {
            this.need_flush = false;
            flush = true;
        }
        else
        {
            this.queue.register(this);
            scope (exit)
                this.queue.unregister(this);

            this.event.wait();
        }
    }

    /***************************************************************************

        Called when waitEvents() signals that the write buffer should be
        flushed. Also informs the channel that a consumer has been flushed.

    ***************************************************************************/

    override protected void flush ( )
    {
        super.flush();
        this.queue.consumerFlushed();
    }

    /***************************************************************************

        DmqListener interface method. Called by Storage when new data arrives
        or channel is deleted.

        Params:
            code = trigger event code

    ***************************************************************************/

    public void trigger ( Code code )
    {
        with (Code) switch (code)
        {
            case DataReady:
                break;
            case Finish:
                this.channel_deleted = true;
                break;
            default:
                break;
        }

        this.event.trigger();
    }
}
