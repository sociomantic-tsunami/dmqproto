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

    private const uint yield_send_count = 10;

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

    private const uint yield_send_count = 10;

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

/*******************************************************************************

    v3 Consume request protocol.

*******************************************************************************/

public abstract scope class ConsumeProtocol_v3
{
    import dmqproto.node.neo.request.core.Mixins;

    import swarm.neo.node.RequestOnConn;
    import dmqproto.common.Consume;

    import swarm.util.RecordBatcher;

    import ocean.transition;

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
        Flush
    }

    /***************************************************************************

        The maximum number of records that should be added to the batch before
        yielding.

    ***************************************************************************/

    private const uint yield_send_count = 10;

    /***************************************************************************

        The minimum size of a batch of records. A batch is sent whenever its
        size is greater than this value after adding one record to the batch.

    ***************************************************************************/

    private const size_t min_batch_length = 100000;

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

        If true, flushBatch() (called when the flush timer expires) resumes the
        fiber.

    ***************************************************************************/

    private bool resume_fiber_on_flush;

    /***************************************************************************

        Aquired buffer in which the most recently popped value is stored.

    ***************************************************************************/

    private void[]* value_buffer;

    /***************************************************************************

        Aquired buffer in which the batch of records to send is stored.

    ***************************************************************************/

    private void[]* record_batch;

    /***************************************************************************

        Request handler. Reads the initial request args and starts the request
        main loop.

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
        this.parser.parseBody(msg_payload, channel_name, subscriber_name);

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
        this.record_batch = this.resources.getVoidBuffer();

        try
        {
            this.run();
            return;
        }
        catch (ChannelRemovedException e)
        {
            // Send the ChannelRemoved message below outside the catch clause to
            // avoid a fiber context switch inside the runtime exception handler.
        }
        finally
        {
            this.stopConsumingChannel(channel_name);
        }

        this.ed.send(
            ( ed.Payload payload )
            {
                payload.addConstant(MessageType_v3.ChannelRemoved);
            }
        );
    }

    /***************************************************************************

        Request main loop. Pops records from the storage, sends them to the
        client in batches and handles Continue/Stop client feedback.

    ***************************************************************************/

    private void run ( )
    {
        while (true)
        {
            while (this.getNextValue(*this.value_buffer))
            {
                size_t record_length = (*this.value_buffer).length;
                (*this.record_batch) ~= (&record_length)[0 .. 1];
                (*this.record_batch) ~= *this.value_buffer;

                if ((*this.record_batch).length >= min_batch_length)
                {
                    if (!this.sendBatchAndReceiveFeedback())
                        return;
                }
            }

            switch (this.wait())
            {
                case NodeFiberResumeCode.Pushed:
                    // Proceed with the `while (getNextValue())` loop to pop,
                    // send the batch if full, then wait again.
                    break;

                case NodeFiberResumeCode.Flush:
                    // Send the incomplete batch, then wait for push or stop.
                    // To wait for flush we can just proceed with the
                    // `while (getNextValue())` loop: `getNextValue` won't pop
                    // anything so that loop will be skipped and we proceed with
                    // waiting for push.
                    if (this.sendBatchAndReceiveFeedback())
                        break;
                    else
                        return;

                case 0:
                    // Received a Stop message.
                    return;

                default:
                    assert(false, "Consume: Unexpected fiber resume code");
            }
        }
    }

    /***************************************************************************

        Called when the storage is empty. Waits for
          - the `Push` signal,
          - the `Flush` signal, if an incomplete batch is pending,
          - a `Stop` message from the client and acknowledges it by sending a
            `Stopped` message.

        Returns:
            - `NodeFiberResumeCode.Pushed` or `NodeFiberResumeCode.Flush` if
              resumed with one of these signals,
            - 0 if a `Stop` message from the client arrived.

    ***************************************************************************/

    private uint wait ( )
    {
        this.resume_fiber_on_push = true;
        this.resume_fiber_on_flush = !!(*this.record_batch).length;

        auto event = this.ed.nextEvent(ed.NextEventFlags.Receive);

        this.resume_fiber_on_push = false;
        this.resume_fiber_on_flush = false;

        switch (event.active)
        {
            case event.active.resumed:
                return event.resumed.code;

            case event.active.received:
                // Received message while the DMQ channel is empty: It should
                // be Stop. Acknowledge Stop and return 0.
                this.verifyReceivedMessageIsStop(event.received.payload);
                this.sendStoppedMessage();
                static assert(NodeFiberResumeCode.min > 0);
                return 0;

            default:
                assert(false, "Consume: Unexpected fiber resume code");
        }
    }

    /***************************************************************************

        Sends the current batch of records; that is, `*this.record_batch`, and
        waits for a `Continue` or `Stop` message. Clears `*this.record_batch`
        when finished.

        Returns:
            `true` if a `Continue` message or `false` if a `Stop` message has
            been received from the client.

    ***************************************************************************/

    private bool sendBatchAndReceiveFeedback ( )
    {
        void fillInRecordsMessage ( ed.Payload payload )
        {
            payload.addConstant(MessageType_v3.Records);
            payload.addArray(*this.record_batch);
        }

        scope (exit)
        {
            (*this.record_batch).length = 0;
            enableStomping(*this.record_batch);
        }

        // Send the records but be ready to potentially receive a Stop message.
        auto event = this.ed.nextEvent(
            ed.NextEventFlags.Receive, &fillInRecordsMessage
        );

        switch (event.active)
        {
            case event.active.sent:
                // Records sent: Wait for Consume/Stop feedback, acknowledge
                // Stop and return true for Continue or false for Stop.
                switch (this.ed.receiveValue!(MessageType_v3)())
                {
                    case MessageType_v3.Continue:
                        return true;

                    case MessageType_v3.Stop:
                        this.sendStoppedMessage();
                        return false;

                    default:
                        throw this.ed.shutdownWithProtocolError(
                            "Consume: Expected Continue or Stopped message " ~
                            "from the client"
                        );
                }

            case event.active.received:
                // Received message before the records have been sent: It should
                // be Stop. Re-send the records, acknowledge Stop and return
                // false.
                this.verifyReceivedMessageIsStop(event.received.payload);
                this.ed.send(&fillInRecordsMessage);
                this.sendStoppedMessage();
                return false;

            default:
                assert(false, "Consume: Unexpected fiber resume code");
        }
    }

    /// Sends a `Stopped` message.
    private void sendStoppedMessage ( )
    {
        this.ed.send(
            (ed.Payload payload)
            {
                payload.addConstant(MessageType_v3.Stopped);
            }
        );
    }

    /***************************************************************************

        Parses `msg_payload`, expecting the message type to be
        `MessageType_v3.Stop`, and raises a protocol error if it is not so.

        Params:
            msg_payload = the payload of a received message

    ***************************************************************************/

    private void verifyReceivedMessageIsStop ( in void[] msg_payload,
        istring file = __FILE__, int line = __LINE__ )
    {
        MessageType_v3 msg_type;
        this.parser.parseBody(msg_payload, msg_type);
        if (msg_type != msg_type.Stop)
            throw this.ed.shutdownWithProtocolError(
                "Consume: Message received from the client is not Stop as expected",
                file, line
            );
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

        Notifies the request when a record has been added to the channel being
        consumed from. The implementing class must call this when notified by
        the storage engine of new data arriving.

    ***************************************************************************/

    final protected void flushBatch ( )
    {
        if ( this.resume_fiber_on_flush )
            this.connection.resumeFiber(NodeFiberResumeCode.Flush);
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
}
