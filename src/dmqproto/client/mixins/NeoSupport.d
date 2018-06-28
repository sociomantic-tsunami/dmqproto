/*******************************************************************************

    Neo protocol support for DmqClient

    Copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.mixins.NeoSupport;

/*******************************************************************************

    Template wrapping access to all "neo" features. Mix this class into a
    DmqClient-derived class and construct the `neo` and 'blocking' objects in
    your constructor.

*******************************************************************************/

template NeoSupport ( )
{
    /***************************************************************************

        Class wrapping access to all "neo" features. (When the old protocol is
        removed, the contents of this class will be moved into the top level of
        the client class.)

        Usage example:
            see the documented unittest, after the class definition

    ***************************************************************************/

    public class Neo
    {
        import swarm.neo.client.mixins.ClientCore;
        import swarm.neo.client.mixins.Controllers;
        import swarm.neo.client.request_options.RequestOptions;

        /***********************************************************************

            Public imports of the request API modules, for the convenience of
            user code.

        ***********************************************************************/

        import Consume = dmqproto.client.request.Consume;
        import Push = dmqproto.client.request.Push;
        import Pop = dmqproto.client.request.Pop;

        /***********************************************************************

            Private imports of the request implementation modules.

        ***********************************************************************/

        private struct Internals
        {
            import dmqproto.client.request.internal.Consume;
            import dmqproto.client.request.internal.Push;
            import dmqproto.client.request.internal.Pop;
        }

        /***********************************************************************

            Mixin core client internals (see
            swarm.neo.client.mixins.ClientCore).

        ***********************************************************************/

        mixin ClientCore!();

        /***********************************************************************

            Mixin `Controller` and `Suspendable` helper class templates (see
            swarm.neo.client.mixins.Controllers).

        ***********************************************************************/

        mixin Controllers!();

        /***********************************************************************

            Test instantiating the `Controller` and `Suspendable` class
            templates.

        ***********************************************************************/

        unittest
        {
            alias Controller!(Consume.IController) ConsumeController;
            alias Suspendable!(Consume.IController) ConsumeSuspendable;
        }

        /***********************************************************************

            DMQ request stats class. New an instance of this class to access
            per-request stats.

        ***********************************************************************/

        public alias RequestStatsTemplate!("Push", "Pop", "Consume") RequestStats;

        /***********************************************************************

            Optional subscriber name parameter for `consume`.

        ***********************************************************************/

        struct Subscriber
        {
            cstring name;
        }

        /***********************************************************************

            Assigns a Consume request. See `dmqproto.request.Consume` for
            detailed documentation.

            Params:
                channel = name of the channel to read from
                notifier = notifier delegate, also called when records arrive
                options = optional `Subscriber` argument

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full

        ***********************************************************************/

        public RequestId consume ( Options ... )
            ( cstring channel, Consume.Notifier notifier, Options options )
        {
            cstring subscriber;
            scope parse_subscriber = (Subscriber sub) { subscriber = sub.name; };
            setupOptionalArgs!(options.length)(options, parse_subscriber);

            auto params = Const!(Internals.Consume.UserSpecifiedParams)(
                Const!(Consume.Args)(channel, subscriber),
                Const!(Internals.Consume.UserSpecifiedParams.SerializedNotifier)(
                    *(cast(Const!(ubyte[notifier.sizeof])*)&notifier)
                )
            );

            auto id = this.assign!(Internals.Consume)(params);
            return id;
        }

        /***********************************************************************

            Assigns a Push request, pushing a value to a single channel. See
            $(LINK2 dmqproto/client/request/Push.html, dmqproto.client.request.Push)
            for detailed documentation.

            Params:
                channel = name of the channel to push to
                value = value to push (will be copied internally)
                notifier = notifier delegate

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full

            TODO: allow optional settings to be specified via varargs

        ***********************************************************************/

        public RequestId push ( cstring channel, Const!(void)[] value,
            Push.Notifier notifier )
        {
            return this.push((&channel)[0 .. 1], value, notifier);
        }

        /***********************************************************************

            Assigns a Push request, pushing a value to one or more channels. See
            $(LINK2 dmqproto/client/request/Push.html, dmqproto.client.request.Push)
            for detailed documentation.

            Params:
                channels = names of the channels to push to (must have at least
                    one and at most 255 elements)
                value = value to push (will be copied internally)
                notifier = notifier delegate

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full

            TODO: allow optional settings to be specified via varargs

        ***********************************************************************/

        public RequestId push ( Const!(char[][]) channels, Const!(void)[] value,
            Push.Notifier notifier )
        {
            // Validate the channels list. The number of channels is transmitted
            // as a ubyte, so the list may contain at most 255 elements.
            enforce(channels.length >= 1, "Push requires at least one channel");
            enforce(channels.length <= 255,
                "Push may operate on at most 255 channels");

            auto params = Const!(Internals.Push.UserSpecifiedParams)(
                Const!(Push.Args)(channels, value),
                Const!(Internals.Push.UserSpecifiedParams.SerializedNotifier)(
                    *(cast(Const!(ubyte[notifier.sizeof])*)&notifier)
                )
            );

            auto id = this.assign!(Internals.Push)(params);
            return id;
        }

        /***********************************************************************

            Assigns a Pop request. See
            $(LINK2 dmqproto/client/request/Pop.html, dmqproto.client.request.Pop)
            for detailed documentation.

            Params:
                channel = name of the channel to pop from
                notifier = notifier delegate

            Returns:
                id of newly assigned request

            Throws:
                NoMoreRequests if the pool of active requests is full

            TODO: allow optional settings to be specified via varargs

        ***********************************************************************/

        public RequestId pop ( cstring channel, Pop.Notifier notifier )
        {
            auto params = Const!(Internals.Pop.UserSpecifiedParams)(
                Const!(Pop.Args)(channel),
                Const!(Internals.Pop.UserSpecifiedParams.SerializedNotifier)(
                    *(cast(Const!(ubyte[notifier.sizeof])*)&notifier)
                )
            );

            auto id = this.assign!(Internals.Pop)(params);
            return id;
        }

        /***********************************************************************

            Gets the type of the wrapper struct of the request associated with
            the specified controller interface.

            Params:
                I = type of controller interface

            Evaluates to:
                the type of the request wrapper struct which contains an
                implementation of the interface I

        ***********************************************************************/

        private template Request ( I )
        {
            static if ( is(I == Consume.IController ) )
            {
                alias Internals.Consume Request;
            }
            else
            {
                static assert(false, I.stringof ~ " does not match any request "
                    ~ "controller");
            }
        }

        /***********************************************************************

            Gets access to a controller for the specified request. If the
            request is still active, the controller is passed to the provided
            delegate for use.

            Important usage notes:
                1. The controller is newed on the stack. This means that user
                   code should never store references to it -- it must only be
                   used within the scope of the delegate.
                2. As the id which identifies the request is only known at run-
                   time, it is not possible to statically enforce that the
                   specified ControllerInterface type matches the request. This
                   is asserted at run-time, though (see
                   RequestSet.getRequestController()).

            Params:
                ControllerInterface = type of the controller interface (should
                    be inferred by the compiler)
                id = id of request to get a controller for (the return value of
                    the method which assigned your request)
                dg = delegate which is called with the controller, if the
                    request is still active

            Returns:
                false if the specified request no longer exists; true if the
                controller delegate was called

        ***********************************************************************/

        public bool control ( ControllerInterface ) ( RequestId id,
            void delegate ( ControllerInterface ) dg )
        {
            alias Request!(ControllerInterface) R;

            return this.controlImpl!(R)(id, dg);
        }

        /***********************************************************************

            Test instantiating the `control` function template.

        ***********************************************************************/

        unittest
        {
            alias control!(Consume.IController) consumeControl;
        }
    }


    /***************************************************************************

        Class wrapping access to all task-blocking "neo" features. (This
        functionality is separated from the main neo functionality as it
        implements methods with the same names and arguments (e.g. a callback-
        based Push request and a task-blocking Push request).)

        Usage example:
            see the documented unittest, after the class definition

    ***************************************************************************/

    private class TaskBlocking
    {
        import swarm.neo.client.mixins.TaskBlockingCore;

        import ocean.core.Array : copy;
        import ocean.task.Task;

        /***********************************************************************

            Mixin core client task-blocking internals (see
            swarm.neo.client.mixins.TaskBlockingCore).

        ***********************************************************************/

        mixin TaskBlockingCore!();

        /***********************************************************************

            Struct returned after a Push request has finished.

        ***********************************************************************/

        private static struct PushResult
        {
            /*******************************************************************

                Set to true if the record was pushed to the DMQ or false if this
                was not possible (i.e. the request attempted to push the record
                to all nodes but all failed).

            *******************************************************************/

            bool succeeded;
        }

        /***********************************************************************

            Assigns a Push request to a single channel and blocks the current
            Task until the request is completed. See
            $(LINK2 dmqproto/client/request/Push.html, dmqproto.client.request.Push)
            for detailed documentation.

            Params:
                channel = name of the channel to push to
                value = value to push (will be copied internally)
                notifier = notifier delegate (optional -- not required for
                    feedback on basic success/failure, but may be desired for
                    more detailed error logging)

            Returns:
                PushResult struct, indicating the result of the request

        ***********************************************************************/

        public PushResult push ( cstring channel, Const!(void)[] value,
            Neo.Push.Notifier notifier = null )
        {
            return this.push((&channel)[0 .. 1], value, notifier);
        }

        /***********************************************************************

            Assigns a Push request to multiple channels and blocks the current
            Task until the request is completed. See
            $(LINK2 dmqproto/client/request/Push.html, dmqproto.client.request.Push)
            for detailed documentation.

            Params:
                channels = names of the channels to push to
                value = value to push (will be copied internally)
                notifier = notifier delegate (optional -- not required for
                    feedback on basic success/failure, but may be desired for
                    more detailed error logging)

            Returns:
                PushResult struct, indicating the result of the request

        ***********************************************************************/

        public PushResult push ( cstring[] channels, Const!(void)[] value,
            Neo.Push.Notifier user_notifier = null )
        {
            auto task = Task.getThis();
            assert(task, "This method may only be called from inside a Task");

            enum FinishedStatus
            {
                None,
                Succeeded,
                Failed
            }

            FinishedStatus state;

            void notifier ( Neo.Push.Notification info, Neo.Push.Args args )
            {
                if ( user_notifier )
                    user_notifier(info, args);

                with ( info.Active ) switch ( info.active )
                {
                    case success:
                        state = state.Succeeded;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case failure:
                        state = state.Failed;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case node_disconnected:
                    case node_error:
                    case unsupported:
                        break;

                    default: assert(false);
                }
            }

            this.outer.neo.push(channels, value, &notifier);
            if ( state == state.None ) // if request not completed, suspend
                task.suspend();
            assert(state != state.None);

            PushResult res;
            res.succeeded = state == state.Succeeded;
            return res;
        }

        /***********************************************************************

            Struct returned after a Pop request has finished.

        ***********************************************************************/

        private static struct PopResult
        {
            /*******************************************************************

                Set to true if one or more nodes were contacted and no errors
                occurred.

            *******************************************************************/

            bool succeeded;

            /*******************************************************************

                The value popped from the channel or an empty array, if the
                channel was empty or an error occurred.

            *******************************************************************/

            void[] value;
        }

        /***********************************************************************

            Assigns a Pop request and blocks the current Task until the request
            is completed. See
            $(LINK2 dmqproto/client/request/Pop.html, dmqproto.client.request.Pop)
            for detailed documentation.

            Params:
                channel = name of the channel to pop from
                value = buffer to receive the popped value (will be set to
                    length 0, if the specified channel is emtpy)
                notifier = notifier delegate (optional -- not required for
                    feedback on basic success/failure, but may be desired for
                    more detailed error logging)

            Returns:
                PopResult struct, indicating the result of the request

        ***********************************************************************/

        public PopResult pop ( cstring channel, ref void[] value,
            Neo.Pop.Notifier user_notifier = null )
        {
            auto task = Task.getThis();
            assert(task, "This method may only be called from inside a Task");

            enum FinishedStatus
            {
                None,
                Succeeded,
                Failed
            }

            FinishedStatus state;

            void notifier ( Neo.Pop.Notification info, Neo.Pop.Args args )
            {
                if ( user_notifier )
                    user_notifier(info, args);

                with ( info.Active ) switch ( info.active )
                {
                    case received:
                        value.copy(info.received.value);
                        state = state.Succeeded;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case empty:
                        state = state.Succeeded;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case not_connected:
                    case failure:
                        state = state.Failed;
                        if ( task.suspended )
                            task.resume();
                        break;

                    case node_disconnected:
                    case node_error:
                    case unsupported:
                    case channel_has_subscribers:
                        break;

                    default: assert(false);
                }
            }

            value.length = 0;
            enableStomping(value);

            this.outer.neo.pop(channel, &notifier);
            if ( state == state.None ) // if request not completed, suspend
                task.suspend();
            assert(state != state.None);

            PopResult res;
            res.value = value;
            res.succeeded = state == state.Succeeded;
            return res;
        }
    }


    /***************************************************************************

        Object containing all neo functionality.

    ***************************************************************************/

    public Neo neo;


    /***************************************************************************

        Object containing all neo task-blocking functionality.

    ***************************************************************************/

    public TaskBlocking blocking;

    /***************************************************************************

        Helper function to initialise neo components.

        Params:
            auth_name = client name for authorisation
            auth_key = client key (password) for authorisation. This should be a
                properly generated random number which only the client and the
                nodes know. See `swarm/README_client_neo.rst` for suggestions.
                The key must be of the length defined in
                swarm.neo.authentication.HmacDef (128 bytes)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )

    ***************************************************************************/

    private void neoInit ( cstring auth_name, ubyte[] auth_key,
        Neo.ConnectionNotifier conn_notifier )
    {
        this.neo = new Neo(auth_name, auth_key, conn_notifier);
        this.blocking = new TaskBlocking;
    }
}
