/*******************************************************************************

    Consume request protocol.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.node.neo.request.Consume;

/*******************************************************************************

    v1 Consume request protocol.

*******************************************************************************/

public abstract scope class ConsumeProtocol_v1
{
    import dmqproto.node.neo.request.core.Mixins;

    import swarm.neo.node.RequestOnConn;
    import swarm.neo.util.StateMachine;
    import dmqproto.common.Consume;

    import ocean.transition;

    /***************************************************************************

        The protocol of this request is implemented as a state machine, with
        messages from the client and changes in the storage engine triggering
        transitions between stats.

        This mixin provides the core state machine functionality and defines the
        states which the request may be in. (The logic for each state is
        implemented in the method of the corresponding name.)

    ***************************************************************************/

    mixin(genStateMachine([
        "Sending",
        "Suspended",
        "WaitingForData"
    ]));

    /***************************************************************************

        Mixin the constructor and resources member.

    ***************************************************************************/

    mixin RequestCore!();

    /***************************************************************************

        Codes used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private enum NodeFiberResumeCode : uint
    {
        Pushed = 1,
        ChannelRemoved = 2
    }

    /***************************************************************************

        The maximum number of records that should be sent in a row before
        yielding.

    ***************************************************************************/

    private static immutable uint yield_send_count = 10;

    /***************************************************************************

        Thrown to cancel the request if the channel was removed.

    ***************************************************************************/

    static class ChannelRemovedException: Exception
    {
        this () {super("Channel removed");}
    }

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    private RequestOnConn connection;

    /***************************************************************************

        Request-on-conn event dispatcher, to send and receive messages.

    ***************************************************************************/

    private RequestOnConn.EventDispatcher ed;

    /***************************************************************************

        Message parser

    ***************************************************************************/

    private RequestOnConn.EventDispatcher.MessageParser parser;

    /***************************************************************************

        If true, dataReady() (called when a record was pushed) resumes the
        fiber.

    ***************************************************************************/

    private bool resume_fiber_on_push;

    /***************************************************************************

        Aquired buffer in which values are stored for sending.

    ***************************************************************************/

    private void[]* value_buffer;

    /***************************************************************************

        Request handler. Reads the initial request args and starts the state
        machine.

        Params:
            connection = connection to client
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    final public void handle ( RequestOnConn connection, Const!(void)[] msg_payload )
    {
        this.connection = connection;
        this.ed = connection.event_dispatcher;
        this.parser = this.ed.message_parser;

        cstring channel_name;
        StartState start_state;
        this.parser.parseBody(msg_payload, channel_name, start_state);

        State state;
        switch ( start_state )
        {
            case StartState.Running:
                state = state.Sending;
                break;
            case StartState.Suspended:
                state = state.Suspended;
                break;
            default:
                this.ed.shutdownWithProtocolError("invalid start state");
        }

        if ( !this.prepareChannel(channel_name) )
        {
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Error);
                }
            );
            return;
        }

        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(RequestStatusCode.Started);
            }
        );

        this.value_buffer = this.resources.getVoidBuffer();

        try
        {
            this.run(state);
            return;
        }
        catch (ChannelRemovedException e)
        {
            // Call sendChannelRemoved() below outside the catch clause to avoid
            // a fiber context switch inside the runtime exception handler.
        }
        finally
        {
            this.stopConsumingChannel(channel_name);
        }

        this.sendChannelRemoved();
    }

    /***************************************************************************

        Performs any logic needed to start consuming from the channel of the
        given name.

        Params:
            channel_name = channel to consume from

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    abstract protected bool prepareChannel ( cstring channel_name );

    /***************************************************************************

        Performs any logic needed to stop consuming from the channel of the
        given name.

        Params:
            channel_name = channel to stop consuming from

    ***************************************************************************/

    abstract protected void stopConsumingChannel ( cstring channel_name );

    /***************************************************************************

        Retrieve the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    abstract protected bool getNextValue ( ref void[] value );

    /***************************************************************************

        Notifies the request when a record has been added to the channel being
        consumed from. The implementing class must call this when notified by
        the storage engine of new data arriving.

    ***************************************************************************/

    final protected void dataReady ( )
    {
        if ( this.resume_fiber_on_push )
            this.connection.resumeFiber(NodeFiberResumeCode.Pushed);
    }

    /***************************************************************************

        Notifies the request when the channel being consumed from is removed.
        The implementing class must call this when notified by the storage
        engine of its imminent demise.

    ***************************************************************************/

    final protected void channelRemoved ( )
    {
        // This happens very rarely, so it is safe to use a new Exception.
        this.connection.resumeFiber(new ChannelRemovedException);
    }

    /***************************************************************************

        Sending state: Pop records from the queue and send them to the client.

    ***************************************************************************/

    private State stateSending ( )
    {
        uint records_sent_without_yielding = 0;

        while ( this.getNextValue(*this.value_buffer) )
        {
            auto next_state = this.sendSingleValue();
            if ( next_state != State.Sending )
                return next_state;

            if (records_sent_without_yielding >= this.yield_send_count)
            {
                bool received_msg;
                MessageType msg_type;

                this.ed.yieldReceiveAndHandleEvents(
                    ( in void[] msg )
                    {
                        this.parser.parseBody(msg, msg_type);
                        received_msg = true;
                    }
                );

                if (received_msg)
                {
                    next_state = this.stateFromMessageType(msg_type);
                    this.sendAck();
                    return next_state;
                }
                records_sent_without_yielding = 0;
            }
            else
            {
                records_sent_without_yielding++;
            }
        }

        return State.WaitingForData;
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

    protected State sendSingleValue ( )
    {
        bool received_msg;
        MessageType msg_type;

        this.ed.sendReceive(
            ( in void[] msg )
            {
                this.parser.parseBody(msg, msg_type);
                received_msg = true;
            },
            ( ed.Payload payload )
            {
                payload.addConstant(MessageType.Record);
                payload.addArray(*this.value_buffer);
            }
        );

        if (received_msg)
        {
            // It's not expected to receive Resume messages while already
            // sending, but it does no harm: The state machine will just
            // call this method again.
            auto next_state = this.stateFromMessageType(msg_type);
            // sendReceive() was interrupted while sending so send again.
            // The client should not send any message until it has received
            // the Ack.
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType.Record);
                    payload.addArray(*this.value_buffer);
                }
            );

            this.sendAck();
            return next_state;
        }

        return State.Sending;
    }

    /***************************************************************************

        Suspended state: Wait until the client resumes or stops the request.

    ***************************************************************************/

    private State stateSuspended ( )
    {
        // It's not expected to receive Suspend messages while already
        // suspended, but it does no harm: The state machine will just call this
        // method again.
        auto next_state = this.stateFromMessageType(
            this.ed.receiveValue!(MessageType)()
        );
        this.sendAck();
        return next_state;
    }

    /***************************************************************************

        WaitingForData state: Wait until either a record is pushed into the
        queue or the client suspends or stops the request.

    ***************************************************************************/

    private State stateWaitingForData ( )
    {
        MessageType msg_type;
        int resume_code;

        this.resume_fiber_on_push = true;
        try
        {
            resume_code = this.ed.receiveAndHandleEvents(
                (in void[] msg) {this.parser.parseBody(msg, msg_type);}
            );
        }
        finally
        {
            this.resume_fiber_on_push = false;
        }

        if (resume_code > 0) // positive code => user code => must be Pushed
        {
            assert(resume_code == NodeFiberResumeCode.Pushed,
                   "Consume: unexpected fiber resume message");
            return State.Sending;
        }

        // We called unregisterConsumer() so the fiber can only be resumed by an
        // I/O event.
        this.sendAck();

        // A Resume message would pointless but acceptable. The state machine
        // will switch to Sending state in this case, which will most likely pop
        // nothing and switch to WaitingForData state again.
        return this.stateFromMessageType(msg_type);
    }

    /***************************************************************************

        Translates `msg_type`, which has been received from the client, into the
        corresponding state. Shuts the connection down if `msg_type` is not a
        control message type.

        Params:
            msg_type = the type of a message received from the client where the
                       client is expected to send a control message

        Returns:
            the state to change to according to `msg_type`.

        Throws:
            `ProtocolError` if `msg_type` is not a control message type.

    ***************************************************************************/

    private State stateFromMessageType ( MessageType msg_type )
    {
        switch (msg_type)
        {
            case MessageType.Suspend:
                return State.Suspended;

            case MessageType.Resume:
                return State.Sending;

            case MessageType.Stop:
                return State.Exit;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "Consume: expected a control message from the client");
        }
    }

    /***************************************************************************

        Sends an `Ack` message to the client. The client is expected to not send
        a message in the mean time or a protocol error is raised.

    ***************************************************************************/

    private void sendAck ( )
    {
        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(MessageType.Ack);
            }
        );

        // Flush the connection to ensure the control message is promptly sent.
        this.ed.flush();
    }


    /***************************************************************************

        Sends a "channel removed" message to the client, ignoring messages
        received from the client. The fiber should not be resumed by consumer
        events.

    ***************************************************************************/

    private void sendChannelRemoved ( )
    {
        bool send_interrupted;

        do
        {
            send_interrupted = false;

            this.ed.sendReceive(
                (in void[] msg) {send_interrupted = true;},
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType.ChannelRemoved);
                }
            );
        }
        while (send_interrupted);

        // Flush the connection to ensure the control message is promptly sent.
        this.ed.flush();
    }
}

/*******************************************************************************

    v2 Consume request protocol.

*******************************************************************************/

public abstract scope class ConsumeProtocol_v2
{
    import dmqproto.node.neo.request.core.Mixins;

    import swarm.neo.node.RequestOnConn;
    import swarm.neo.util.StateMachine;
    import dmqproto.common.Consume;

    import ocean.transition;

    /***************************************************************************

        The protocol of this request is implemented as a state machine, with
        messages from the client and changes in the storage engine triggering
        transitions between stats.

        This mixin provides the core state machine functionality and defines the
        states which the request may be in. (The logic for each state is
        implemented in the method of the corresponding name.)

    ***************************************************************************/

    mixin(genStateMachine([
        "Sending",
        "Suspended",
        "WaitingForData"
    ]));

    /***************************************************************************

        Mixin the constructor and resources member.

    ***************************************************************************/

    mixin RequestCore!();

    /***************************************************************************

        Codes used when resuming the fiber to interrupt waiting for I/O.

    ***************************************************************************/

    private enum NodeFiberResumeCode : uint
    {
        Pushed = 1,
        ChannelRemoved = 2
    }

    /***************************************************************************

        The maximum number of records that should be sent in a row before
        yielding.

    ***************************************************************************/

    private static immutable uint yield_send_count = 10;

    /***************************************************************************

        Thrown to cancel the request if the channel was removed.

    ***************************************************************************/

    static class ChannelRemovedException: Exception
    {
        this () {super("Channel removed");}
    }

    /***************************************************************************

        Request-on-conn, to get the event dispatcher and control the fiber.

    ***************************************************************************/

    private RequestOnConn connection;

    /***************************************************************************

        Request-on-conn event dispatcher, to send and receive messages.

    ***************************************************************************/

    private RequestOnConn.EventDispatcher ed;

    /***************************************************************************

        Message parser

    ***************************************************************************/

    private RequestOnConn.EventDispatcher.MessageParser parser;

    /***************************************************************************

        If true, dataReady() (called when a record was pushed) resumes the
        fiber.

    ***************************************************************************/

    private bool resume_fiber_on_push;

    /***************************************************************************

        Aquired buffer in which values are stored for sending.

    ***************************************************************************/

    private void[]* value_buffer;

    /***************************************************************************

        Request handler. Reads the initial request args and starts the state
        machine.

        Params:
            connection = connection to client
            msg_payload = initial message read from client to begin the request
                (the request code and version are assumed to be extracted)

    ***************************************************************************/

    final public void handle ( RequestOnConn connection, Const!(void)[] msg_payload )
    {
        this.connection = connection;
        this.ed = connection.event_dispatcher;
        this.parser = this.ed.message_parser;

        cstring channel_name;
        cstring subscriber_name;
        StartState start_state;
        this.parser.parseBody(msg_payload, channel_name, subscriber_name, start_state);

        State state;
        switch ( start_state )
        {
            case StartState.Running:
                state = state.Sending;
                break;
            case StartState.Suspended:
                state = state.Suspended;
                break;
            default:
                this.ed.shutdownWithProtocolError("invalid start state");
        }

        if ( !this.prepareChannel(channel_name, subscriber_name) )
        {
            this.ed.send(
                ( ed.Payload payload )
                {
                    payload.addConstant(RequestStatusCode.Error);
                }
            );
            return;
        }

        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(RequestStatusCode.Started);
            }
        );

        this.value_buffer = this.resources.getVoidBuffer();

        try
        {
            this.run(state);
            return;
        }
        catch (ChannelRemovedException e)
        {
            // Call sendChannelRemoved() below outside the catch clause to avoid
            // a fiber context switch inside the runtime exception handler.
        }
        finally
        {
            this.stopConsumingChannel(channel_name);
        }

        this.sendChannelRemoved();
    }

    /***************************************************************************

        Performs any logic needed to subscribe to and start consuming from the
        channel of the given name.

        Params:
            channel_name = channel to consume from
            subscriber_name = the identifying name of the subscriber

        Returns:
            `true` if the channel may be used

    ***************************************************************************/

    abstract protected bool prepareChannel ( cstring channel_name,
                                             cstring subscriber_name );

    /***************************************************************************

        Performs any logic needed to stop consuming from the channel of the
        given name.

        Params:
            channel_name = channel to stop consuming from

    ***************************************************************************/

    abstract protected void stopConsumingChannel ( cstring channel_name );

    /***************************************************************************

        Retrieve the next value from the channel, if available.

        Params:
            value = buffer to write the value into

        Returns:
            `true` if there was a value in the channel, false if the channel is
            empty

    ***************************************************************************/

    abstract protected bool getNextValue ( ref void[] value );

    /***************************************************************************

        Notifies the request when a record has been added to the channel being
        consumed from. The implementing class must call this when notified by
        the storage engine of new data arriving.

    ***************************************************************************/

    final protected void dataReady ( )
    {
        if ( this.resume_fiber_on_push )
            this.connection.resumeFiber(NodeFiberResumeCode.Pushed);
    }

    /***************************************************************************

        Notifies the request when the channel being consumed from is removed.
        The implementing class must call this when notified by the storage
        engine of its imminent demise.

    ***************************************************************************/

    final protected void channelRemoved ( )
    {
        // This happens very rarely, so it is safe to use a new Exception.
        this.connection.resumeFiber(new ChannelRemovedException);
    }

    /***************************************************************************

        Sending state: Pop records from the queue and send them to the client.

    ***************************************************************************/

    private State stateSending ( )
    {
        uint records_sent_without_yielding = 0;

        while ( this.getNextValue(*this.value_buffer) )
        {
            bool received_msg;
            MessageType msg_type;

            this.ed.sendReceive(
                ( in void[] msg )
                {
                    this.parser.parseBody(msg, msg_type);
                    received_msg = true;
                },
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType.Record);
                    payload.addArray(*this.value_buffer);
                }
            );

            if (received_msg)
            {
                // It's not expected to receive Resume messages while already
                // sending, but it does no harm: The state machine will just
                // call this method again.
                auto next_state = this.stateFromMessageType(msg_type);
                // sendReceive() was interrupted while sending so send again.
                // The client should not send any message until it has received
                // the Ack.
                this.ed.send(
                    ( ed.Payload payload )
                    {
                        payload.addConstant(MessageType.Record);
                        payload.addArray(*this.value_buffer);
                    }
                );

                this.sendAck();
                return next_state;
            }

            if (records_sent_without_yielding >= this.yield_send_count)
            {
                received_msg = false;
                this.ed.yieldReceiveAndHandleEvents(
                    ( in void[] msg )
                    {
                        this.parser.parseBody(msg, msg_type);
                        received_msg = true;
                    }
                );

                if (received_msg)
                {
                    auto next_state = this.stateFromMessageType(msg_type);
                    this.sendAck();
                    return next_state;
                }
                records_sent_without_yielding = 0;
            }
            else
            {
                records_sent_without_yielding++;
            }
        }

        return State.WaitingForData;
    }

    /***************************************************************************

        Suspended state: Wait until the client resumes or stops the request.

    ***************************************************************************/

    private State stateSuspended ( )
    {
        // It's not expected to receive Suspend messages while already
        // suspended, but it does no harm: The state machine will just call this
        // method again.
        auto next_state = this.stateFromMessageType(
            this.ed.receiveValue!(MessageType)()
        );
        this.sendAck();
        return next_state;
    }

    /***************************************************************************

        WaitingForData state: Wait until either a record is pushed into the
        queue or the client suspends or stops the request.

    ***************************************************************************/

    private State stateWaitingForData ( )
    {
        MessageType msg_type;
        int resume_code;

        this.resume_fiber_on_push = true;
        try
        {
            resume_code = this.ed.receiveAndHandleEvents(
                (in void[] msg) {this.parser.parseBody(msg, msg_type);}
            );
        }
        finally
        {
            this.resume_fiber_on_push = false;
        }

        if (resume_code > 0) // positive code => user code => must be Pushed
        {
            assert(resume_code == NodeFiberResumeCode.Pushed,
                   "Consume: unexpected fiber resume message");
            return State.Sending;
        }

        // We called unregisterConsumer() so the fiber can only be resumed by an
        // I/O event.
        this.sendAck();

        // A Resume message would pointless but acceptable. The state machine
        // will switch to Sending state in this case, which will most likely pop
        // nothing and switch to WaitingForData state again.
        return this.stateFromMessageType(msg_type);
    }

    /***************************************************************************

        Translates `msg_type`, which has been received from the client, into the
        corresponding state. Shuts the connection down if `msg_type` is not a
        control message type.

        Params:
            msg_type = the type of a message received from the client where the
                       client is expected to send a control message

        Returns:
            the state to change to according to `msg_type`.

        Throws:
            `ProtocolError` if `msg_type` is not a control message type.

    ***************************************************************************/

    private State stateFromMessageType ( MessageType msg_type )
    {
        switch (msg_type)
        {
            case MessageType.Suspend:
                return State.Suspended;

            case MessageType.Resume:
                return State.Sending;

            case MessageType.Stop:
                return State.Exit;

            default:
                throw this.ed.shutdownWithProtocolError(
                    "Consume: expected a control message from the client");
        }
    }

    /***************************************************************************

        Sends an `Ack` message to the client. The client is expected to not send
        a message in the mean time or a protocol error is raised.

    ***************************************************************************/

    private void sendAck ( )
    {
        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(MessageType.Ack);
            }
        );

        // Flush the connection to ensure the control message is promptly sent.
        this.ed.flush();
    }


    /***************************************************************************

        Sends a "channel removed" message to the client, ignoring messages
        received from the client. The fiber should not be resumed by consumer
        events.

    ***************************************************************************/

    private void sendChannelRemoved ( )
    {
        bool send_interrupted;

        do
        {
            this.ed.sendReceive(
                (in void[] msg) {send_interrupted = true;},
                ( ed.Payload payload )
                {
                    payload.addConstant(MessageType.ChannelRemoved);
                }
            );
        }
        while (send_interrupted);

        // Flush the connection to ensure the control message is promptly sent.
        this.ed.flush();
    }
}
