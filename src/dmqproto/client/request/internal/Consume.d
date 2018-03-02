/*******************************************************************************

    Client DMQ Consume v3 request handler.

    Copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.internal.Consume;

import ocean.transition;
import ocean.core.Verify;

import swarm.neo.client.RequestOnConn;

/*******************************************************************************

    Consume v3 request implementation.

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
    import dmqproto.common.Consume;
    import dmqproto.client.request.Consume;
    import dmqproto.common.RequestCodes;
    import swarm.neo.client.mixins.RequestCore;
    import swarm.neo.client.mixins.AllNodesRequestCore;
    import swarm.neo.client.mixins.BatchRequestCore;
    import swarm.neo.client.RequestHandlers;
    import dmqproto.client.internal.SharedResources;

    import ocean.io.select.protocol.generic.ErrnoIOException: IOError;

    import ocean.transition;
    mixin TypeofThis!();

    /***************************************************************************

        Request controller, accessible to the user via the client's `control()`
        method.

    ***************************************************************************/

    mixin BatchController!(typeof(this), IController);

    /***************************************************************************

        Data which the request needs while it is in progress. An instance of
        this struct is stored per connection on which the request runs and is
        passed to the request handler.

    ***************************************************************************/

    private struct SharedWorking
    {
        /// Shared working data required for core all-nodes request behaviour.
        AllNodesRequestSharedWorkingData all_nodes;

        /// Data required by the BatchController
        BatchRequestSharedWorkingData suspendable_control;
    }

    /***************************************************************************

        Data which each request-on-conn needs while it is progress. An instance
        of this struct is stored per connection on which the request runs and is
        passed to the request handler.

    ***************************************************************************/

    private struct Working { } // Dummy struct.

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.AllNodes, RequestCode.Consume, 3, Args,
        SharedWorking, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conn = request-on-conn event dispatcher
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled

    ***************************************************************************/

    public static void handler ( RequestOnConn.EventDispatcherAllNodes conn,
        void[] context_blob )
    {
        auto context = This.getContext(context_blob);

        auto shared_resources = SharedResources.fromObject(
            context.shared_resources);
        scope acquired_resources = shared_resources.new RequestResources;
        scope handler = new ConsumeHandler(conn, context, acquired_resources);
        handler.run();
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlerFinished().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob )
    {
        auto context = This.getContext(context_blob);

        // Final notification, after the request has been stopped
        Notification notification;
        notification.stopped = RequestInfo(context.request_id);
        Consume.notify(context.user_params, notification);
    }
}

/*******************************************************************************

    Client Consume v3 request handler.

*******************************************************************************/

private scope class ConsumeHandler
{
    import swarm.neo.connection.RequestOnConnBase;
    import swarm.neo.client.mixins.AllNodesRequestCore;
    import swarm.neo.client.mixins.BatchRequestCore;
    import swarm.neo.request.RequestEventDispatcher;
    import swarm.neo.request.Command;
    import swarm.neo.util.MessageFiber;

    import dmqproto.common.Consume;
    import dmqproto.client.request.Consume;
    import dmqproto.client.internal.SharedResources;

    alias Consume.BatchRequestSharedWorkingData.Signal ControllerSignal;

    /// Request-on-conn event dispatcher.
    private RequestOnConn.EventDispatcherAllNodes conn;

    /// Request context.
    private Consume.Context* context;

    /// Request resource acquirer.
    private SharedResources.RequestResources resources;

    /// Request event dispatcher.
    private RequestEventDispatcher request_event_dispatcher;

    /***************************************************************************

        Constructor.

        Params:
            conn = request-on-conn event dispatcher to communicate with node
            context = deserialised request context
            resources = request resource acquirer

    ***************************************************************************/

    public this ( RequestOnConn.EventDispatcherAllNodes conn,
        Consume.Context* context, SharedResources.RequestResources resources )
    {
        this.conn = conn;
        this.context = context;
        this.resources = resources;
        this.request_event_dispatcher.initialise(&resources.getBuffer);
    }

    /***************************************************************************

        Main request handling entry point.

    ***************************************************************************/

    public void run ( )
    {
        auto initialiser = createAllNodesRequestInitialiser!(Consume)(
            this.conn, this.context, &this.fillPayload);
        auto request = createAllNodesRequest!(Consume)(this.conn, this.context,
            &this.connect, &this.disconnected, initialiser, &this.handle);
        request.run();
    }

    /***************************************************************************

        Connect policy, called from AllNodesRequest template to ensure the
        connection to the node is up.

        Returns:
            true to continue handling the request; false to abort

    ***************************************************************************/

    private bool connect ( )
    {
        return batchRequestConnector(this.conn);
    }

    /***************************************************************************

        Disconnected policy, called from AllNodesRequest template when an I/O
        error occurs on the connection.

        Params:
            e = exception indicating error which occurred on the connection

    ***************************************************************************/

    private void disconnected ( Exception e )
    {
        // Notify the user of the disconnection. The user may use the
        // controller, at this point, but as the request is not active
        // on this connection, no special behaviour is needed.
        Consume.Notification notification;
        notification.node_disconnected =
            NodeExceptionInfo(this.conn.remote_address, e);
        Consume.notify(this.context.user_params, notification);
    }

    /***************************************************************************

        FillPayload policy, called from AllNodesRequestInitialiser template
        to add request-specific data to the initial message payload send to the
        node to begin the request.

        Params:
            payload = message payload to be filled

    ***************************************************************************/

    private void fillPayload ( RequestOnConnBase.EventDispatcher.Payload payload )
    {
        payload.addArray(this.context.user_params.args.channel);
        payload.addArray(this.context.user_params.args.subscriber);
    }

    /***************************************************************************

        Handler policy, called from AllNodesRequest template to run the
        request's main handling logic.

    ***************************************************************************/

    private void handle ( )
    {
        // Handle initial started/error message from node.
        switch (conn.receiveValue!(RequestStatusCode)())
        {
            case RequestStatusCode.Started:
                // Expected "request started" code
                break;

            // Treat unknown codes as internal errors.
            case RequestStatusCode.Error:
            default:
                // The node returned an error code. Notify the user and
                // end the request.
                Consume.Notification n;
                n.node_error = NodeInfo(conn.remote_address);
                Consume.notify(this.context.user_params, n);
                return;
        }

        scope record_stream = this.new RecordStream;
        scope reader = this.new Reader(record_stream);
        scope controller = this.new Controller(record_stream);

        this.request_event_dispatcher.eventLoop(this.conn);

        with (record_stream.fiber)  verify(state == state.TERM);
        with (reader.fiber)         verify(state == state.TERM);
        with (controller.fiber)     verify(state == state.TERM);
    }

    /***************************************************************************

        Codes for signals sent across the fibers.

    ***************************************************************************/

    enum FiberSignal: ubyte
    {
        /// Resumes the `RecordStream` fiber.
        ResumeRecordStream = ControllerSignal.max + 1,
        /// Tells the `Controller` to terminate.
        StopController
    }

    /***************************************************************************

        The fiber that waits for a batch of records to arrive and passes it to
        the user, then sends the `Continue` message to the node, in a loop.
        Handles suspending the request through the controller, for resuming call
        `resume`. Calling `stop` makes this routine terminate after all
        remaining records have been passed to the user.

    ***************************************************************************/

    private class RecordStream
    {
        /// The acquired buffer to store a batch of records.
        private void[]* batch_buffer;

        /// Slices the records in *batch_buffer that haven't been processed yet.
        private Const!(void)[] remaining_batch = null;

        /// The fiber.
        private MessageFiber fiber;

        /// Tells if the fiber is suspended, and if yes, what it is waiting for.
        enum FiberSuspended: uint
        {
            No,
            WaitingForRecords,
            RequestSuspended
        }

        /// Ditto
        private FiberSuspended fiber_suspended;

        /***********************************************************************

            If true, causes the fiber to exit after processing remaining records
            in the batch. Set if the stop method is called.

        ***********************************************************************/

        private bool stopped = false;

        /// Token passed to fiber suspend/resume calls.
        private static MessageFiber.Token token =
            MessageFiber.Token(typeof(this).stringof);

        /// Constructor, starts the fiber.
        private this ( )
        {
            this.batch_buffer = this.outer.resources.getBuffer();
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /***********************************************************************

            Adds a batch of records to be passed to the user notifier and
            resumes the fiber if it is waiting for more records. Called by the
            `Reader` when a `Records` message from the node has arrived.

            Params:
                record_batch = the batch of records to add

        ***********************************************************************/

        public void addRecords ( in void[] record_batch )
        {
            // Append record_batch to *this.batch_buffer, which may or may not
            // be empty.

            if (!(*this.batch_buffer).length)
                enableStomping(*this.batch_buffer);

            // Append record_batch, then set this.remaining_batch to reference
            // the remaining records. To avoid a dangling slice if
            // *this.batch_buffer is relocated, set this.remaining_batch to null
            // first.
            size_t n_processed = (*this.batch_buffer).length - this.remaining_batch.length;
            this.remaining_batch = null;
            (*this.batch_buffer) ~= record_batch;
            this.remaining_batch = (*this.batch_buffer)[n_processed .. $];

            if (this.fiber_suspended == fiber_suspended.WaitingForRecords)
                this.resumeFiber();
        }

        /***********************************************************************

            Resumes passing records to the user. Called by `Controller` when the
            user resumes the request through the controller.

        ***********************************************************************/

        public void resume ( )
        {
            if (this.fiber_suspended == fiber_suspended.RequestSuspended)
                this.resumeFiber();
        }

        /***********************************************************************

            Requests the fiber to terminate when all remaining records have been
            passed to the user. Called when a `Stopped` message from the node
            has arrived.

        ***********************************************************************/

        public void stop ( )
        {
            this.stopped = true;
            if (this.fiber_suspended == fiber_suspended.WaitingForRecords)
                this.resumeFiber();
        }

        /***********************************************************************

            Waits for a batch of records to be fed to it by the `Reader` and
            passes it to the user, then sends the `Continue` message to the
            node, in a loop. Handles suspending the request through the
            controller, for resuming call `resume`. Calling `stop` makes this
            routine terminate after all remaining records have been passed to
            the user.

        ***********************************************************************/

        private void fiberMethod ( )
        {
            while (this.waitForRecords())
            {
                for (uint yield_count = 0; this.remaining_batch.length; yield_count++)
                {
                    if (yield_count >= 10) // yield every 10th record
                    {
                        yield_count = 0;
                        this.outer.request_event_dispatcher.yield(this.fiber);
                    }

                    if (this.outer.context.shared_working.suspendable_control.suspended)
                    {
                        yield_count = 0;
                        this.suspendFiber(FiberSuspended.RequestSuspended);
                    }

                    this.passRecordToUser(
                        this.outer.conn.message_parser.getArray
                        !(Const!(void))(this.remaining_batch)
                    );
                }

                this.remaining_batch = null;
                (*this.batch_buffer).length = 0;

                if (this.stopped)
                    break;

                this.outer.request_event_dispatcher.send(
                    this.fiber,
                    (conn.Payload payload)
                    {
                        payload.addCopy(MessageType.Continue);
                    }
                );
            }

            this.outer.request_event_dispatcher.signal(this.outer.conn,
                FiberSignal.StopController);
        }

        /***********************************************************************

            Suspends the fiber to be resumed by `addRecords` or `stop`.

            Returns:
                true if the fiber was resumed by `addRecords` or false if
                resumed by `stop`.

        ***********************************************************************/

        private bool waitForRecords ( )
        {
            this.suspendFiber(FiberSuspended.WaitingForRecords);
            return !this.stopped;
        }

        /***********************************************************************

            Calls the user notifier to pass `record` to the user. Handles a
            request state change (i.e. stopping the request) if the user uses
            the controller in the notifier.

            Params:
                record = the record to pass to the user

        ***********************************************************************/

        private void passRecordToUser ( in void[] record )
        {
            bool initially_stopped =
                this.outer.context.shared_working.suspendable_control.stopped;

            Notification notification;
            notification.received = RequestDataInfo(this.outer.context.request_id, record);
            Consume.notify(this.outer.context.user_params, notification);

            if (!initially_stopped &&
                this.outer.context.shared_working.suspendable_control.stopped
            )
                this.outer.request_event_dispatcher.signal(this.outer.conn,
                    ControllerSignal.Stop);
        }

        /***********************************************************************

            Suspends the fiber, waiting for `FiberSignal.ResumeRecordStream`.
            `why` specifies the current state of the fiber method and determins
            which of the public methods should raise that signal.

            Params:
                why = the event on which the fiber method needs to be resumed

        ***********************************************************************/

        private void suspendFiber ( FiberSuspended why )
        {
            this.fiber_suspended = why;
            try
                this.outer.request_event_dispatcher.nextEvent(this.fiber,
                    Signal(FiberSignal.ResumeRecordStream));
            finally
                this.fiber_suspended = fiber_suspended.No;
        }

        /***********************************************************************

            Raises `FiberSignal.ResumeRecordStream` to resume the fiber.

        ***********************************************************************/

        private void resumeFiber ( )
        {
            this.outer.request_event_dispatcher.signal(this.outer.conn,
                FiberSignal.ResumeRecordStream);
        }
    }

    /***************************************************************************

        The fiber that reads messages from the node and notifies `RecordStream`.

    ***************************************************************************/

    private class Reader
    {
        /// The fiber.
        private MessageFiber fiber;

        /// The `RecordStream` to notify when a message has arrived.
        private RecordStream record_stream;

        /***********************************************************************

            Constructor, starts the fiber.

            Params:
                record_stream = the `RecordStream` to notify when a message has
                    arrived

        ***********************************************************************/

        private this ( RecordStream record_stream )
        {
            this.record_stream = record_stream;
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /***********************************************************************

            Reads messages from the node and notifies `record_strem` by calling
            its respective methods when a `Records` or `Stopped` message has
            arrived. Quits when a `Stopped` message has arrived because
            `Stopped` is the last message from the node.

        ***********************************************************************/

        private void fiberMethod ( )
        {
            while (true)
            {
                auto msg = this.outer.request_event_dispatcher.receive(
                    this.fiber,
                    Message(MessageType.Records),
                    Message(MessageType.Stopped),
                    Message(MessageType.ChannelRemoved)
                );

                final switch (msg.type)
                {
                    case MessageType.Records:
                        Const!(void)[] received_record_batch;
                        this.outer.conn.message_parser.parseBody(
                            msg.payload, received_record_batch
                        );
                        this.record_stream.addRecords(received_record_batch);
                        break;

                    case MessageType.Stopped:
                        this.record_stream.stop();
                        return;

                    case MessageType.ChannelRemoved:
                        // TODO
                        break;

                    version (D_Version2) {} else default:
                        assert(false);
                }
            }
        }
    }

    /***************************************************************************

        The fiber that handles user controller signals.

    ***************************************************************************/

    private class Controller
    {
        /// The fiber.
        private MessageFiber fiber;

        /// The `RecordStream` to notify when the request is resumed.
        private RecordStream record_stream;

        /***********************************************************************

            Constructor, starts the fiber.

            Params:
                record_stream = the `RecordStream` to notify when the request is
                    resumed

        ***********************************************************************/

        private this ( RecordStream record_stream )
        {
            this.record_stream = record_stream;
            this.fiber = this.outer.resources.getFiber(&this.fiberMethod);
            this.fiber.start();
        }

        /***********************************************************************

            Waits for controller signals and handles them. Terminates on
            `FiberSignal.StopController`.

        ***********************************************************************/

        private void fiberMethod ( )
        {
            while (true)
            {
                auto event = this.outer.request_event_dispatcher.nextEvent(
                    this.fiber,
                    Signal(ControllerSignal.Resume),
                    Signal(ControllerSignal.Stop),
                    Signal(FiberSignal.StopController)
                );

                final switch (event.signal.code)
                {
                    case ControllerSignal.Resume:
                        this.record_stream.resume();
                        break;

                    case ControllerSignal.Stop:
                        this.outer.request_event_dispatcher.send(
                            this.fiber,
                            (conn.Payload payload)
                            {
                                payload.addCopy(MessageType.Stop);
                            }
                        );
                        break;

                    case FiberSignal.StopController:
                        return;

                    version (D_Version2) {} else default:
                        assert(false);
                }
            }
        }
    }
}
