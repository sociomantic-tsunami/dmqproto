/*******************************************************************************

    DMQ client usage examples.

    Note that the examples are only for the neo requests.

    Copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.UsageExamples;

version ( UnitTest )
{
    import ocean.transition;

    import dmqproto.client.DmqClient;

    import ocean.io.select.EpollSelectDispatcher;
    import ocean.task.Scheduler;
    import ocean.task.Task;
    import ocean.io.Stdout;
    import ocean.io.model.SuspendableThrottlerCount;

    import swarm.neo.authentication.HmacDef : Key;

    /***************************************************************************

        Helper struct containing a DMQ client, an epoll instance, and basic
        initialisation/connection code (This struct serves to avoid repeating
        all the initialisation/connection boilerplate in every example. It is
        not intended that real applications need such a struct. Typically,
        its members would be in your main app class.)

    ***************************************************************************/

    struct DmqInit
    {
        public EpollSelectDispatcher epoll;
        public DmqClient dmq;

        /***********************************************************************

            Create a DMQ client and connect to the neo nodes defined in the
            "dmq.nodes" file.

            Note that this method does not actually wait until the connections
            to one/all nodes have been fully established. In most real
            applications, you would want to do so. See the Task-blocking
            examples for one way to do this.

            Params:
                epoll = epoll instance to use inside DMQ client

        ***********************************************************************/

        public void connect ( EpollSelectDispatcher epoll )
        {
            // Create an epoll instance.
            this.epoll = epoll;

            // Create a DMQ client instance, passing the additional
            // arguments required by neo: the authorisation name and
            // password and the connection notifier (see below).
            auto auth_name = "neotest";
            ubyte[] auth_key = Key.init.content;
            this.dmq = new DmqClient(this.epoll, auth_name, auth_key,
                &this.connNotifier);

            // Add some nodes.
            // Note: make sure you have a .nodes file which specifies the
            // neo ports of the nodes!
            dmq.neo.addNodes("dmq.nodes");
        }

        // Notifier which is called when a connection establishment attempt
        // succeeds or fails. (Also called after re-connection attempts are
        // made.)
        private void connNotifier ( DmqClient.Neo.ConnNotification info )
        {
            with ( info.Active ) switch ( info.active )
            {
                case connected:
                    Stdout.formatln("Connected to {}:{}",
                        info.connected.node_addr.address_bytes,
                        info.connected.node_addr.port);
                    break;

                case error_while_connecting:
                    Stderr.formatln("Connection error '{}' on {}:{}",
                        getMsg(info.error_while_connecting.e),
                        info.error_while_connecting.node_addr.address_bytes,
                        info.error_while_connecting.node_addr.port);
                    break;

                default:
                    assert(false);
            }
        }
    }
}

/*******************************************************************************

    Dummy struct to enable ddoc rendering of usage examples.

*******************************************************************************/

struct UsageExamples
{
}

/// Full example of neo Push request usage
unittest
{
    struct PushExample
    {
        private DmqInit dmq_init;

        // Method which initialises the client and starts a request
        public void start ( )
        {
            this.dmq_init.connect(new EpollSelectDispatcher);

            // Assign a neo Push request. Note that the channel and value
            // are copied inside the client -- the user does not need to
            // maintain them after calling this method.
            this.dmq_init.dmq.neo.push(["channel"], "value_to_push",
                &this.pushNotifier);

            // Start the event loop to set it all running.
            this.dmq_init.epoll.eventLoop();
        }

        // Notifier which is called when something of interest happens to
        // the Push request. See dmqproto.client.request.Push for details of
        // the parameters of the notifier. (Each request has a module like
        // this, defining its public API.)
        private void pushNotifier ( DmqClient.Neo.Push.Notification info,
            DmqClient.Neo.Push.Args args )
        {
            // `info` is a smart union, where each member of the union
            // represents one possible notification. `info.active` denotes
            // the type of the current notification. Some notifications
            // have fields containing more information:
            with ( info.Active ) switch ( info.active )
            {
                case success:
                    Stdout.formatln("The request succeeded!");
                    break;

                case failure:
                    Stdout.formatln("The request failed on all nodes.");
                    break;

                case node_disconnected:
                    Stdout.formatln(
                        "The request failed due to connection error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case node_error:
                    Stdout.formatln(
                        "The request failed due to a node error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    // If there are more nodes left to try, the request will
                    // be retried automatically.
                    break;

                case unsupported:
                    switch ( info.unsupported.type )
                    {
                        case info.unsupported.type.RequestNotSupported:
                            Stdout.formatln(
                                "The request is not supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        case info.unsupported.type.RequestVersionNotSupported:
                            Stdout.formatln(
                                "The request version is not supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        default: assert(false);
                    }
                    break;

                default: assert(false);
            }
        }
    }
}

/// Full example of Task-blocking neo Push request usage
unittest
{
    // Task-derivative which waits for connection establishment and performs
    // a Push request
    static class PushTask : Task
    {
        private DmqClient dmq;

        this ( DmqClient dmq )
        {
            this.dmq = dmq;
        }

        override public void run ( )
        {
            // Wait for all connections to be established
            this.dmq.blocking.waitAllNodesConnected();

            // Perform a blocking Push request.
            // Note that you may also pass a notifier to Task-blocking
            // requests, e.g. for the purposes of detailed logging.
            auto result = this.dmq.blocking.push("channel", "value_to_push");
            Stdout.formatln("Push request {}",
                result.succeeded ? "succeeded" : "failed");
        }
    }

    // Function which initialises the client and starts a request
    void start ( )
    {
        DmqInit dmq_init;
        SchedulerConfiguration config;
        initScheduler(config);
        dmq_init.connect(theScheduler.epoll);

        // Start a Push task
        theScheduler.schedule(new PushTask(dmq_init.dmq));
        theScheduler.eventLoop();
    }
}

/// Full example of neo Consume request usage, including using the controller
unittest
{
    struct ConsumeExample
    {
        import ocean.io.Stdout;

        private DmqInit dmq_init;

        // Id of the running Consume request
        private DmqClient.Neo.RequestId rq_id;

        // Method which initialises the client and starts a request
        public void start ( )
        {
            this.dmq_init.connect(new EpollSelectDispatcher);

            // Assign a neo Consume request. Note that we store the id of
            // the request (the return value). This can be used to control
            // the request, as it's in progress (see the `received` case in
            // consumerNotifier(), below).
            this.rq_id = this.dmq_init.dmq.neo.consume("channel",
                &this.consumeNotifier,
                dmq_init.dmq.neo.Subscriber("subscriber"));

            // Start the event loop to set it all running.
            this.dmq_init.epoll.eventLoop();
        }

        // Notifier which is called when something of interest happens to
        // the Consume request. See dmqproto.client.request.Consume for
        // details of the parameters of the notifier. (Each request has a
        // module like this, defining its public API.)
        private void consumeNotifier (
            DmqClient.Neo.Consume.Notification info,
            DmqClient.Neo.Consume.Args args )
        {
            // `info` is a smart union, where each member of the union
            // represents one possible notification. `info.active` denotes
            // the type of the current notification. Some notifications
            // have fields containing more information:
            with ( info.Active ) switch ( info.active )
            {
                case started:
                    Stdout.formatln("The request started on all nodes.");
                    break;

                case received:
                    Stdout.formatln("'{}' received from channel '{}'",
                        cast(char[])info.received.value, args.channel);

                    // Here we use the controller to cleanly end the request
                    // after a while
                    static ubyte count;
                    if ( ++count >= 10 )
                        this.stop();
                    break;

                case stopped:
                    Stdout.formatln("The request stopped on all nodes.");
                    break;

                case suspended:
                    Stdout.formatln("The request suspended on all nodes.");
                    break;

                case resumed:
                    Stdout.formatln("The request resumed on all nodes.");
                    break;

                case channel_removed:
                    Stdout.formatln(
                        "The request ended as the channel was removed.");
                    break;

                case node_disconnected:
                    Stdout.formatln(
                        "The request failed due to connection error {} on {}:{}",
                        getMsg(info.node_disconnected.e),
                        info.node_disconnected.node_addr.address_bytes,
                        info.node_disconnected.node_addr.port);
                    break;

                case node_error:
                    Stdout.formatln(
                        "The request failed due to a node error on {}:{}",
                        info.node_error.node_addr.address_bytes,
                        info.node_error.node_addr.port);
                    break;

                case unsupported:
                    switch ( info.unsupported.type )
                    {
                        case info.unsupported.type.RequestNotSupported:
                            Stdout.formatln(
                                "The request is not supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        case info.unsupported.type.RequestVersionNotSupported:
                            Stdout.formatln(
                                "The request version is not supported by node {}:{}",
                                info.unsupported.node_addr.address_bytes,
                                info.unsupported.node_addr.port);
                            break;
                        default: assert(false);
                    }
                    break;

                default: assert(false);
            }
        }

        // Method which is called from the `received` case of the notifier
        // (above). Sends a message to the DMQ to cleanly stop handling this
        // request.
        private void stop ( )
        {
            // The control() method of the client allows you to get access
            // to an interface providing methods which control the state of
            // a request, while it's in progress. The Consume request
            // controller interface is in dmqproto.client.request.Consume.
            // Not all requests can be controlled in this way.
            this.dmq_init.dmq.neo.control(this.rq_id,
                ( DmqClient.Neo.Consume.IController consume )
                {
                    // We tell the request to stop. This will cause a
                    // message to be sent to all DMQ nodes, telling them to
                    // end the Consume. More records may be received while
                    // this is happening, but the notifier is called, as
                    // soon as all nodes have stopped. (There are also
                    // controller methods to suspend and resume the request
                    // on the node-side.)
                    consume.stop();
                }
            );
        }
    }
}

/// Example of using a Consume controller with a suspendable throttler.
/// (Not a full usage example. See above for more detailed examples of basic
/// DMQ client usage.)
unittest
{
    void controlTest ( DmqClient dmq )
    {
        // Start a Consume request
        auto request_id = dmq.neo.consume(
            "channel",
            ( dmq.neo.Consume.Notification, dmq.neo.Consume.Args ) { },
            dmq.neo.Subscriber("subscriber")
        );

        // Get a Suspendable interface to the Consume request
        auto suspendable_consume = dmq.neo.
            new Suspendable!(DmqClient.Neo.Consume.IController)(request_id);

        // Set up a throttler and add the Consume suspender to it.
        // Note that, if the Consume request finishes (for whatever reason),
        // the suspendable will throw, if used. To avoid this, it should be
        // removed from the throttler, when the request finishes.
        auto throttler = new SuspendableThrottlerCount(100, 10);
        throttler.addSuspendable(suspendable_consume);
    }
}

/// Example of using the stats APIs
unittest
{
    void getStats ( DmqClient dmq )
    {
        // See Stats in swarm.neo.client.mixins.ClientCore
        auto stats = dmq.neo.new Stats;

        // Connection stats.
        Stdout.formatln("DMQ nodes registered with client: {}",
            stats.num_registered_nodes);
        Stdout.formatln("DMQ nodes in initial connection establishment state: {}",
            stats.num_initializing_nodes);
        Stdout.formatln("Current fraction of DMQ nodes in initial connection establishment state: {}",
            stats.initializing_nodes_fraction);
        Stdout.formatln("DMQ nodes connected: {}",
            stats.num_connected_nodes);
        Stdout.formatln("All DMQ nodes connected?: {}",
            stats.all_nodes_connected);
        Stdout.formatln("Current fraction of DMQ nodes connected: {}",
            stats.connected_nodes_fraction);

        // Connection I/O stats.
        {
            size_t i;
            foreach ( conn_sender_io, conn_receiver_io; stats.connection_io )
            {
                // See swarm.neo.protocol.socket.IOStats
                Stdout.formatln("Total bytes sent/received over connection {}: {}",
                    i++, conn_sender_io.socket.total, conn_receiver_io.socket.total);
            }
        }

        // Connection send queue stats.
        {
            size_t i;
            foreach ( send_queue; stats.connection_send_queue )
            {
                // See swarm.neo.util.TreeQueue
                Stdout.formatln("Total time messages waited in send queue of connection {}: {}μs",
                    i++, send_queue.time_histogram.total_time_micros);
            }
        }

        // Request pool stats.
        Stdout.formatln("Requests currently active: {}",
            stats.num_active_requests);
        Stdout.formatln("Maximum active requests allowed: {}",
            stats.max_active_requests);
        Stdout.formatln("Current fraction of maximum active requests: {}",
            stats.active_requests_fraction);

        // Per-request stats.
        auto rq_stats = dmq.neo.new RequestStats;
        foreach ( name, stats; rq_stats.allRequests() )
        {
            // See swarm.neo.client.requests.Stats
            Stdout.formatln("{} {} requests handled, mean time: {}μs",
                stats.count, name, stats.mean_handled_time_micros);
        }
    }
}
