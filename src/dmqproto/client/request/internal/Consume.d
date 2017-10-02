/*******************************************************************************

    Client DMQ Consume v1 request handler.

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.internal.Consume;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;
import ocean.util.log.Log;
import dmqproto.common.Consume;

/*******************************************************************************

    Module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dmqproto.client.request.internal.Consume");
}

/*******************************************************************************

    Consume request implementation.

    Note that request structs act simply as namespaces for the collection of
    symbols required to implement a request. They are never instantiated and
    have no fields or non-static functions.

    The client expects several things to be present in a request struct:
        1. The static constants request_type and request_code
        2. The UserSpecifiedParams struct, containing all user-specified request
            setup (including a notifier)
        3. The Notifier delegate type
        4. Optionally, the Controller type (if the request can be controlled,
           after it has begun)
        5. The handler() function
        6. The all_finished_notifier() function

    The RequestCore mixin provides items 1 and 2.

*******************************************************************************/

public struct Consume
{
    import dmqproto.client.request.Consume;
    import dmqproto.common.RequestCodes;
    import swarm.neo.client.mixins.RequestCore;

    import ocean.io.select.protocol.generic.ErrnoIOException: IOError;

    /***************************************************************************

        Request controller, accessible to the user via the client's `control()`
        method.

    ***************************************************************************/

    public static scope class Controller : IController
    {
        import ocean.core.Enforce;

        /***********************************************************************

            Base mixin.

        ***********************************************************************/

        mixin ControllerBase;

        /***********************************************************************

            Custom fiber resume code, used when the request handling fiber is
            resumed by the controller.

        ***********************************************************************/

        private enum ConsumeFiberResumeCode
        {
            ControlMessage = 3
        }

        /***********************************************************************

            Tells the nodes to stop sending data to this request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool suspend ( )
        {
            return this.changeDesiredState(MessageType.Suspend);
        }

        /***********************************************************************

            Tells the nodes to resume sending data to this request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool resume ( )
        {
            return this.changeDesiredState(MessageType.Resume);
        }

        /***********************************************************************

            Tells the nodes to cleanly end the request.

            Returns:
                false if the controller cannot be used because a control change
                is already in progress

        ***********************************************************************/

        override public bool stop ( )
        {
            return this.changeDesiredState(MessageType.Stop);
        }

        /***********************************************************************

            Changes the desired state to that specified. Sets the desired state
            flag and resumes any handler fibers which are suspended, passing the
            control message flag to the fiber via the return value of suspend().

            If one or more connections are not ready to change state, the
            control change doesn not occur. A connection is ready to change the
            request state unless the handler is currently waiting for an
            acknowledgement message when beginning the request or changing its
            state.

            Params:
                code = desried state

            Returns:
                true if the state change has been accepted and will be sent to
                all active nodes, false if one or more connections is already in
                the middle of changing state

        ***********************************************************************/

        private bool changeDesiredState ( MessageType code )
        {
            auto context = Consume.getContext(this.request_controller.context_blob);

            if (context.shared_working.handlers_waiting_for_ack)
                return false;

            auto info = RequestInfo(context.request_id);
            Notification notification;

            // Set the desired state in the shared working data
            with ( MessageType ) switch ( code )
            {
                case Resume:
                    context.shared_working.desired_state =
                        SharedWorking.DesiredState.Running;
                    notification.resumed = info;
                    break;
                case Suspend:
                    context.shared_working.desired_state =
                        SharedWorking.DesiredState.Suspended;
                    notification.suspended = info;
                    break;
                case Stop:
                    context.shared_working.desired_state =
                        SharedWorking.DesiredState.Stopped;
                    notification.stopped = info;
                    break;

                default: assert(false,
                    "Consume.Controller: Unexpected message type");
            }

            // If one or more connections are ready to send a state change
            // message to the node, we initiate this.
            if (context.shared_working.handlers_ready_for_state_change)
            {
                this.request_controller.resumeSuspendedHandlers(
                    ConsumeFiberResumeCode.ControlMessage);
            }
            // If no connections are ready to send state change messages, the
            // state change essentially occurs immediately (without the need for
            // node contact). We just call the notifier.
            else
            {
                Consume.notify(context.user_params, notification);
            }

            return true;
        }
    }

    /***************************************************************************

        Data which the request needs while it is progress. An instance of this
        struct is stored in the request's context (which is passed to the
        request handler).

    ***************************************************************************/

    private static struct SharedWorking
    {
        private enum DesiredState
        {
            None,
            Running,
            Suspended,
            Stopped
        }

        public DesiredState desired_state = DesiredState.Running;

        /***********************************************************************

            The number of handlers that are currently waiting for an
            acknowledgement message from the node after the request was started
            or its state changed. Using the controller to request changing the
            request state is possible if and only if this number is 0. Whenever
            this number is counted down to 0, the user notifier is called
            because at that point all available nodes have acknowledged the
            start or a state change of the request.

        ***********************************************************************/

        public uint handlers_waiting_for_ack;

        /***********************************************************************

            Flag set when the user's started notifier has been called. This is
            needed to ensure that this notification only occurs once.
            (Otherwise, it would be repeated if a connection died and was
            reestablished, for example.)

        ***********************************************************************/

        public bool called_started_notifier;

        /***********************************************************************

            The number of handlers that are currently ready to send state change
            messages to the node. A handler is not ready for state changes if
            the connection is down or the node returned an error status code. We
            track this state as it is required in Controller.changeDesiredState,
            where we need to decide whether to resume waiting handlers.

        ***********************************************************************/

        public uint handlers_ready_for_state_change;
    }

    /***************************************************************************

        Data which each request-on-conn needs while it is progress. An instance
        of this struct is stored per connection on which the request runs and is
        passed to the request handler.

    ***************************************************************************/

    private static struct Working
    {
        MessageType requested_control_msg = MessageType.None;
    }

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.AllNodes, RequestCode.Consume, 2,
        Args, SharedWorking, Working, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conn = connection event dispatcher
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled
            working_blob = untyped chunk of data containing the serialized
                working data for the request on this connection

    ***************************************************************************/

    public static void handler ( RequestOnConn.EventDispatcherAllNodes conn,
        void[] context_blob, void[] working_blob )
    {
        scope h = new Handler(conn, context_blob);

        bool reconnect;
        do
        {
            reconnect = false;

            try
            {
                h.run(h.State.EstablishingConnection);
            }
            // Only retry in the case of a connection error. Other errors
            // indicate internal problems and should not be retried.
            catch (IOError e)
            {
                // Reset the working data of this connection to the initial state.
                auto working = Consume.getWorkingData(working_blob);
                *working = Working.init;

                // Notify the user of the disconnection. The user may use the
                // controller, at this point, but as the request is not active
                // on this connection, no special behaviour is needed.
                Notification notification;
                notification.node_disconnected =
                    NodeExceptionInfo(conn.remote_address, e);
                Consume.notify(h.context.user_params, notification);

                reconnect = true;
            }
            finally
            {
                h.setNotReadyForStateChange();
            }
        }
        while ( reconnect );
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlerFinished().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing
            working_data_iter = iterator over the stored working data associated
                with each connection on which this request was run

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob,
        IRequestWorkingData working_data_iter )
    {
        auto context = This.getContext(context_blob);

        // Final notification, after the request has been stopped
        Notification notification;
        notification.stopped = RequestInfo(context.request_id);
        Consume.notify(context.user_params, notification);
    }
}

/*******************************************************************************

    Consume handler class instantiated inside the main handler() function,
    above.

*******************************************************************************/

private scope class Handler
{
    import swarm.neo.util.StateMachine;
    import dmqproto.client.request.Consume;
    import swarm.neo.request.Command : StatusCode;
    import dmqproto.common.RequestCodes;
    import swarm.neo.client.RequestOnConn;
    import swarm.neo.util.StateMachine;

    /***************************************************************************

        Mixin core of state machine.

    ***************************************************************************/

    mixin(genStateMachine([
        "EstablishingConnection",
        "Initialising",
        "Receiving",
        "RequestingStateChange"
    ]));

    /***************************************************************************

        Event dispatcher for this connection.

    ***************************************************************************/

    private RequestOnConn.EventDispatcherAllNodes conn;

    /***************************************************************************

        Deserialized request context.

    ***************************************************************************/

    public Consume.Context* context;

    /***************************************************************************

        True while this handler can send a state change message to the node.
        The actual purpose of this flag is for `setNotReadyForStateChange()` to
        tell whether `context.shared_working.handlers_ready_for_state_change`
        was already decremented.

    ***************************************************************************/

    private bool ready_for_state_change;

    /***************************************************************************

        Constructor.

        Params:
            conn = Event dispatcher for this connection
            context_blob = serialized request context

    ***************************************************************************/

    public this ( RequestOnConn.EventDispatcherAllNodes conn,
                  void[] context_blob )
    {
        this.conn = conn;
        this.context = Consume.getContext(context_blob);
    }

    /***************************************************************************

        Waits for the connection to be established if it is down.

        Next state:
            - Initialising (by default)
            - Exit if the desired state becomes Stopped, while connection is in
              progress.

        Returns:
            next state

    ***************************************************************************/

    private State stateEstablishingConnection ( )
    {
        while (true)
        {
            switch (this.conn.waitForReconnect())
            {
                case conn.FiberResumeCodeReconnected:
                case 0: // The connection is already up
                    return State.Initialising;

                case Consume.Controller.ConsumeFiberResumeCode.ControlMessage:
                    if (this.context.shared_working.desired_state ==
                        this.context.shared_working.desired_state.Stopped)
                        // The user requested to stop this request, so we don't
                        // need to wait for a reconnection any more.
                        return State.Exit;
                    else
                        break;

                default:
                    assert(false,
                        typeof(this).stringof ~ ".stateWaitingForReconnect: " ~
                        "Unexpected fiber resume code when reconnecting");
            }
        }
    }

    /***************************************************************************

        Sends the request code, version, channel, etc. to the node to begin the
        request, then receives the status code from the node (unless the desired
        state is Stopped; nothing is done in that case).

        Next state:
            - Receiving (by default)
            - RequestingStateChange if the user changed the state in the
              notifier
            - Exit if the desired state is Stopped

        Returns:
            next state

    ***************************************************************************/

    private State stateInitialising ( )
    in
    {
        assert(!this.ready_for_state_change);
    }
    out (state)
    {
        if (state != state.Exit)
            assert(this.ready_for_state_change);
        else
            assert(!this.ready_for_state_change);
    }
    body
    {
        // Figure out what starting state to tell the node to begin handling the
        // request in.
        StartState start_state;
        with (this.context.shared_working) switch (desired_state)
        {
            case desired_state.Running:
                start_state = start_state.Running;
                break;

            case desired_state.Suspended:
                start_state = start_state.Suspended;
                break;

            case desired_state.Stopped:
                return State.Exit;

            default:
                assert(false, typeof(this).stringof ~ ".stateInitialising: invalid desired state");
        }

        // Memorize the state that will be sent to the node in order to detect a
        // state change in the user's notifier.
        auto last_state = this.context.shared_working.desired_state;

        try
        {
            // stateWaitingForReconnect should guarantee we're already connected
            assert(this.conn.waitForReconnect() == 0);

            // We know that the connection is up, so from now on we count this
            // request among those which the started notification depends on.
            this.context.shared_working.handlers_waiting_for_ack++;

            // Send request info to node
            this.conn.send(
                ( conn.Payload payload )
                {
                    payload.add(Consume.cmd.code);
                    payload.add(Consume.cmd.ver);
                    payload.addArray(this.context.user_params.args.channel);
                    payload.addArray(this.context.user_params.args.subscriber);
                    payload.add(start_state);
                }
            );

            // Receive status from node and stop the request if not Ok
            auto status = conn.receiveValue!(StatusCode)();
            if ( !Consume.handleGlobalStatusCodes(status, context,
                conn.remote_address) )
            {
                switch ( status )
                {
                    case RequestStatusCode.Started:
                        break;

                    case RequestStatusCode.Error:
                        // The node returned an error code. Notify the user and
                        // end the request on this connection.
                        Notification n;
                        n.node_error = NodeInfo(this.conn.remote_address);
                        Consume.notify(this.context.user_params, n);
                        return State.Exit;

                default:
                    log.warn("Received unknown status code {} from node in "
                        ~ "response to Consume request. Treating as Error.",
                        status);
                    goto case RequestStatusCode.Error;
                }
            }
        }
        finally
        {
            assert(this.context.shared_working.handlers_waiting_for_ack);
            --this.context.shared_working.handlers_waiting_for_ack;
        }

        // Now we're ready to receive records from the node or to handle state
        // change requests from the user via the controller.
        this.ready_for_state_change = true;
        this.context.shared_working.handlers_ready_for_state_change++;

        // Notify the user when all connections are running.
        if (!this.context.shared_working.called_started_notifier &&
            !this.context.shared_working.handlers_waiting_for_ack)
        {
            this.context.shared_working.called_started_notifier = true;
            Notification notification;
            notification.started = RequestInfo(this.context.request_id);
            Consume.notify(this.context.user_params, notification);
        }

        // After successfully starting the request, the notifier delegate is
        // called. The user may use the controller, at this point, so we need to
        // check for newly requested state changes, i.e. if desired_state
        // changed.
        return (last_state == this.context.shared_working.desired_state)
            ? State.Receiving
            : State.RequestingStateChange;
    }

    /***************************************************************************

        Default running state. Receives one record message from the node and
        passes it to the user's notifier delegate.

        Next state:
            - again Receiving (by default)
            - RequestingStateChange if the user changed the state

        Returns:
            next state

    ***************************************************************************/

    private State stateReceiving ( )
    {
        // Inside handleMessage(), the notifier delegate is called. The
        // user may use the controller, at this point, so we need to check
        // for newly requested state changes, i.e. if desired_state changed.
        auto last_state = this.context.shared_working.desired_state;
        MessageType received_msg_type;
        auto resume_code = this.conn.receiveAndHandleEvents(
            ( in void[] received )
            {
                received_msg_type = this.handleMessage(received);
            }
        );

        if ( resume_code < 0 )
        {
            switch (received_msg_type)
            {
                case received_msg_type.Record:
                    // The received record has been sent to the user (see
                    // handleMessage())
                    break;

                case received_msg_type.ChannelRemoved:
                    Notification notification;
                    notification.channel_removed =
                        NodeInfo(this.conn.remote_address);
                    Consume.notify(this.context.user_params, notification);
                    return State.Exit;

                default:
                    throw this.conn.shutdownWithProtocolError(
                        "Expected message type Record or ChannelRemoved");
            }
        }

        return (last_state == this.context.shared_working.desired_state)
            ? State.Receiving
            : State.RequestingStateChange;
    }

    /***************************************************************************

        Sends a request state change message to the node and waits for the
        acknowledgement, handling records arriving in the mean time, as normal.

        If the node connection breaks while sending the state change message or
        receiving the acknowledgement, the state change was successful because
        the request will be restarted with the requested state.

        Next state:
            - Receiving by default, i.e. if the desired state is Running or
              Suspended
            - Exit if the desired state is Stopped
            - again RequestingStateChange if the user changed the state in the
              notifier

        Returns:
            next state

    ***************************************************************************/

    private State stateRequestingStateChange ( )
    {
        // Based on the desired state, decide which control message to send to
        // the node and which notification type to use.
        MessageType control_msg;
        Notification notification;
        this.stateChangeMsgAndNotification(control_msg, notification);

        this.context.shared_working.handlers_waiting_for_ack++;

        // Memorize the state that will be sent to the node in order to detect a
        // state change in the user's notifier.
        auto signaled_state = this.context.shared_working.desired_state;

        try
        {
            // If throwing, set this request not ready for a state change
            // *before* calling the notifier in the `finally` clause, where
            // the user may request a state change.
            scope (failure) this.setNotReadyForStateChange();

            // Send the control message to the node and handle incoming messages.
            while (true)
            {
                bool send_interrupted;
                MessageType received_msg_type;

                // Though the user's notifier delegate may be called at this point,
                // we do not check for state changes as the controller enforces that
                // a state change may not be requested while the last is in progress.
                this.conn.sendReceive(
                    ( in void[] received )
                    {
                        send_interrupted = true;
                        received_msg_type = this.handleMessage(received);
                    },
                    ( conn.Payload payload )
                    {
                        payload.add(control_msg);
                    }
                );

                if ( !send_interrupted ) // The control message was sent
                    break;

                // Sending the control message was interrupted by a received
                // message
                switch (received_msg_type)
                {
                    case received_msg_type.Record:
                        // The received record has been sent to the user (see
                        // handleMessage())
                        break;

                    case received_msg_type.ChannelRemoved:
                        notification.channel_removed =
                            NodeInfo(this.conn.remote_address);
                        Consume.notify(this.context.user_params, notification);
                        return State.Exit;

                    case received_msg_type.Ack:
                    default:
                        throw this.conn.shutdownWithProtocolError(
                            "Expected message type Record or ChannelRemoved");
                }
            }

            // Flush the connection to ensure the control message is promptly
            // sent.
            this.conn.flush();

            // Receive the Ack message while handling incoming record messages.
            WaitForAck: while (true)
            {
                MessageType received_msg_type;
                this.conn.receive(
                    ( in void[] received )
                    {
                        received_msg_type = this.handleMessage(received);
                    }
                );

                switch (received_msg_type)
                {
                    case received_msg_type.Ack:
                        break WaitForAck;

                    case received_msg_type.Record:
                        // Continue receiving until an Ack message arrives.
                        break;

                    case received_msg_type.ChannelRemoved:
                        notification.channel_removed =
                            NodeInfo(this.conn.remote_address);
                        Consume.notify(this.context.user_params, notification);
                        return State.Exit;

                    default:
                        throw this.conn.shutdownWithProtocolError(
                            "Expected message type Ack, Record, or ChannelRemoved");
                }
            }
        }
        finally
        {
            assert(this.context.shared_working.handlers_waiting_for_ack);
            if (!--this.context.shared_working.handlers_waiting_for_ack)
            {
                // If this was the last connection waiting for the
                // acknowledgement, inform the user that the requested control
                // message has taken effect.
                // If stopped then the notification is done in
                // Consume.all_finished_notifier so don't do it here.
                if (notification.active != notification.active.stopped)
                    Consume.notify(this.context.user_params, notification);
            }
        }

        // After successfully changing the request state, the notifier delegate
        // is called. The user may use the controller, at this point, so we need
        // to check for newly requested state changes and try again.
        return (signaled_state == this.context.shared_working.desired_state)
            ? (signaled_state == signaled_state.Stopped)
                ? State.Exit
                : State.Receiving
            : State.RequestingStateChange;
    }

    /***************************************************************************

        Helper function for stateRequestingStateChange(). Besed on the currently
        desired request state, determines:
            1. the MessageType to send to the node
            2. the type of notification to send to the user, once the request
               has changed state on all nodes

        Params:
            control_msg = set to the MessageType to send to the node
            notification = set to the notification to send to the user

    ***************************************************************************/

    private void stateChangeMsgAndNotification ( out MessageType control_msg,
        out Notification notification )
    {
        with ( this.context.shared_working ) switch ( desired_state )
        {
            case desired_state.Running:
                control_msg = MessageType.Resume;
                notification.resumed = RequestInfo(this.context.request_id);
                break;

            case desired_state.Suspended:
                control_msg = MessageType.Suspend;
                notification.suspended = RequestInfo(this.context.request_id);
                break;

            case desired_state.Stopped:
                control_msg = MessageType.Stop;
                notification.stopped = RequestInfo(this.context.request_id);
                break;

            default: assert(false, typeof(this).stringof ~
                ".stateChangeMsgAndNotification: " ~
                "Unexpected desired state requested");
        }
    }

    /***************************************************************************

        Helper function to handle messages received from the node. Messages
        containing records are passed to the user's delegate receiving
        (specified in the request params).

        Params:
            payload = raw message payload received from the node

        Returns:
            The message type.

    ***************************************************************************/

    private MessageType handleMessage ( Const!(void)[] payload )
    {
        auto msg_type = *this.conn.message_parser.getValue!(MessageType)(payload);

        if (msg_type == msg_type.Record)
        {
            Const!(void)[] record;
            this.conn.message_parser.parseBody(payload, record);
            Notification notification;
            notification.received = RequestDataInfo(context.request_id, record);
            Consume.notify(this.context.user_params, notification);
        }

        return msg_type;
    }

    /***************************************************************************

        Decrements `context.shared_working.handlers_ready_for_state_change` if
        `this.ready_for_state_change` is true (i.e. it hasn't already been
        decremented).

    ***************************************************************************/

    private void setNotReadyForStateChange ( )
    {
        if (this.ready_for_state_change)
        {
            assert(this.context.shared_working.handlers_ready_for_state_change);
            --this.context.shared_working.handlers_ready_for_state_change;
            this.ready_for_state_change = false;
        }
    }

    /***************************************************************************

        Debug message, printed on state change.

    ***************************************************************************/

    debug (ClientConsumeState):

    import ocean.io.Stdout;

    private void beforeState ( )
    {
        static char[][] machine_msg =
        [
            State.WaitingForReconnect: "WaitingForReconnect",
            State.Initialising: "Initialising",
            State.Receiving: "Receiving",
            State.RequestingStateChange: "RequestingStateChange",
            State.Exit: "Exit"
        ];

        alias typeof(this.context.shared_working.desired_state) DesiredState;

        static char[][] request_msg =
        [
            DesiredState.None: "???",
            DesiredState.Running: "Running",
            DesiredState.Suspended: "Suspended",
            DesiredState.Stopped: "Stopped"
        ];

        Stdout.green.formatln("Consume state: Machine = {}, Request = {}",
            machine_msg[this.state],
            request_msg[this.context.shared_working.desired_state]).default_colour;
    }
}
