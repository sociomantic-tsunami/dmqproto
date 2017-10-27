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
import dmqproto.node.neo.request.core.IRequestResources;

import fakedmq.Storage;
import fakedmq.neo.SharedResources;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;

/*******************************************************************************

    The request handler for the table of handlers. When called, runs in a fiber
    that can be controlled via `connection`.

    Params:
        shared_resources = an opaque object containing resources owned by the
            node which are required by the request
        connection  = performs connection socket I/O and manages the fiber
        cmdver      = the version number of the Consume command as specified by
                      the client
        msg_payload = the payload of the first message of this request

*******************************************************************************/

public void handle ( Object shared_resources, RequestOnConn connection,
    Command.Version cmdver, Const!(void)[] msg_payload )
{
    auto resources = new SharedResources;

    switch ( cmdver )
    {
        case 1:
            scope rq = new ConsumeImpl_v1(resources);
            rq.handle(connection, msg_payload);
            break;

        case 2:
            scope rq = new ConsumeImpl_v2(resources);
            rq.handle(connection, msg_payload);
            break;

        default:
            auto ed = connection.event_dispatcher;
            ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(GlobalStatusCode.RequestVersionNotSupported);
                }
            );
            break;
    }
}

/*******************************************************************************

    Fake node implementation of the v1 Consume request protocol.

*******************************************************************************/

private scope class ConsumeImpl_v1 : ConsumeProtocol_v1, DmqListener
{
    /***************************************************************************

        Remember consumed channel

    ***************************************************************************/

    private Queue queue;

    /***************************************************************************

        Constructor.

        Params:
            shared_resources = DMQ request resources getter

    ***************************************************************************/

    public this ( IRequestResources resources)
    {
        super(resources);
    }

    /***************************************************************************

        Performs any logic needed to start consuming from the channel of the
        given name.

        Params:
            channel_name = channel to subscribe to
            subscriber_name = subscriber name (v2 only)

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    override protected bool prepareChannel ( cstring channel_name )
    {
        this.queue = global_storage.getCreate(channel_name).subscribe("");
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
            return false;

        value = this.queue.pop().dup;
        return true;
    }

    /***************************************************************************

        Helper function to send a single value and handle messages received from
        the client in the meantime.

        (Note that this method is protected as implementations of the protocol
        may wish to add extra behaviour at the point of sending a single value.)

        Returns:
            If the value was sent without a message being received,
            State.Sending. Otherwise, the state to transition to (as determined
            by the message received).

    ***************************************************************************/

    override protected State sendSingleValue ( )
    {
        auto next_state = super.sendSingleValue();

        this.queue.consumerFlushed();

        return next_state;
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
            case Finish:
                this.channelRemoved();
                break;
            default:
                break;
        }
    }
}

/*******************************************************************************

    Fake node implementation of the v2 Consume request protocol.

*******************************************************************************/

private scope class ConsumeImpl_v2 : ConsumeProtocol_v2, DmqListener
{
    /***************************************************************************

        Remember consumed channel

    ***************************************************************************/

    private Queue queue;

    /***************************************************************************

        Constructor.

        Params:
            shared_resources = DMQ request resources getter

    ***************************************************************************/

    public this ( IRequestResources resources)
    {
        super(resources);
    }

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
            case Finish:
                this.channelRemoved();
                break;
            default:
                break;
        }
    }
}
