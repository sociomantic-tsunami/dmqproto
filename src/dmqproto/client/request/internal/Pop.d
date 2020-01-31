/*******************************************************************************

    Client DMQ Pop v1 request handler.

    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.request.internal.Pop;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.meta.types.Qualifiers;
import ocean.util.log.Logger;

/*******************************************************************************

    Module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dmqproto.client.request.internal.Pop");
}

/*******************************************************************************

    Pop request implementation.

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

public struct Pop
{
    import dmqproto.common.Pop;
    import dmqproto.client.request.Pop;
    import dmqproto.common.RequestCodes;
    import swarm.neo.client.mixins.RequestCore;
    import swarm.neo.client.RequestHandlers : IRoundRobinConnIterator;
    import swarm.neo.request.Command;

    import ocean.io.select.protocol.generic.ErrnoIOException: IOError;

    /***************************************************************************

        Data which the request needs while it is progress. An instance of this
        struct is stored per connection on which the request runs and is passed
        to the request handler.

    ***************************************************************************/

    private static struct SharedWorking
    {
        /// Tried to perform the request on at least one node
        bool tried = false;

        /// Received a value from a node.
        bool received = false;

        /// At least one node returned an error status code.
        bool error = false;
    }

    /***************************************************************************

        Data which each request-on-conn needs while it is progress. An instance
        of this struct is stored per connection on which the request runs and is
        passed to the request handler.

    ***************************************************************************/

    private static struct Working
    {
        // Dummy (not required by this request)
    }

    /***************************************************************************

        Request core. Mixes in the types `NotificationInfo`, `Notifier`,
        `Params`, `Context` plus the static constants `request_type` and
        `request_code`.

    ***************************************************************************/

    mixin RequestCore!(RequestType.RoundRobin, RequestCode.Pop, 1, Args,
        SharedWorking, Notification);

    /***************************************************************************

        Request handler. Called from RequestOnConn.runHandler().

        Params:
            conns = round-robin getter for per-connection event dispatchers
            context_blob = untyped chunk of data containing the serialized
                context of the request which is to be handled

    ***************************************************************************/

    public static void handler ( IRoundRobinConnIterator conns,
        void[] context_blob )
    {
        auto context = Pop.getContext(context_blob);
        context.shared_working = SharedWorking.init;

        foreach (conn; conns)
        {
            context.shared_working.tried = true;

            try
            {
                // Send request info to node
                conn.send(
                    ( conn.Payload payload )
                    {
                        payload.add(Pop.cmd.code);
                        payload.add(Pop.cmd.ver);
                        payload.addArray(context.user_params.args.channel);
                    }
                );

                auto supported = conn.receiveValue!(SupportedStatus)();
                if ( Pop.handleSupportedCodes(supported, context,
                    conn.remote_address) )
                    // Read the status code and (optionally) the popped value.
                    conn.receive(
                        ( const(void)[] const_payload )
                        {
                            const(void)[] payload = const_payload;
                            auto status =
                                *conn.message_parser.getValue!(StatusCode)(payload);

                            switch ( status )
                            {
                                case RequestStatusCode.Popped:
                                    // Read the popped value and pass it to the
                                    // user's notifier.
                                    auto value =
                                        conn.message_parser.getArray!(void)(
                                            payload);

                                    Notification notification;
                                    notification.received =
                                        RequestDataInfo(context.request_id, value);
                                    Pop.notify(context.user_params, notification);

                                    context.shared_working.received = true;
                                    break;

                                case RequestStatusCode.Empty:
                                    // The channel is empty on this node. Try
                                    // another connection.
                                    break;

                                case RequestStatusCode.Subscribed:
                                    // The node can't pop from the channel
                                    // because the channel has subscribers.
                                    // Notify the user and try another
                                    // connection.
                                    Notification n;
                                    n.channel_has_subscribers =
                                        NodeInfo(conn.remote_address);
                                    Pop.notify(context.user_params, n);

                                    context.shared_working.error = true;
                                    break;

                                case RequestStatusCode.Error:
                                    // The node returned an error code. Notify
                                    // the user and try another connection.
                                    Notification n;
                                    n.node_error = NodeInfo(conn.remote_address);
                                    Pop.notify(context.user_params, n);

                                    context.shared_working.error = true;
                                    break;

                                default:
                                    log.warn("Received unknown status code {} "
                                        ~ "from node in response to Pop request. "
                                        ~ "Treating as Error.", status);
                                    goto case RequestStatusCode.Error;
                            }
                        }
                    );
                else
                    context.shared_working.error = true;

                // Once we receive a record, exit the loop.
                if ( context.shared_working.received )
                    break;
            }
            catch ( IOError e )
            {
                // A connection error occurred. Notify the user and try another
                // connection.
                Notification n;
                n.node_disconnected = NodeExceptionInfo(conn.remote_address, e);
                Pop.notify(context.user_params, n);
            }
        }
    }

    /***************************************************************************

        Request finished notifier. Called from Request.handlerFinished().

        Params:
            context_blob = untyped chunk of data containing the serialized
                context of the request which is finishing

    ***************************************************************************/

    public static void all_finished_notifier ( void[] context_blob )
    {
        auto context = Pop.getContext(context_blob);

        // Final notification (in failure or empty cases -- the received case is
        // handled in handler(), above).
        if ( !context.shared_working.received )
        {
            auto info = RequestInfo(context.request_id);
            Notification n;

            if ( !context.shared_working.tried )
                // no nodes connected
                n.not_connected = info;
            else if ( context.shared_working.error )
                // one or more nodes returned an error status
                n.failure = info;
            else
                // didn't receive a value or an error; all nodes must be empty
                n.empty = info;

            Pop.notify(context.user_params, n);
        }
    }
}
