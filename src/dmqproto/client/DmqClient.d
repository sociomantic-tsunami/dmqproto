/*******************************************************************************

    Asynchronous/event-driven DMQ client using non-blocking socket I/O (epoll)

    The neo support is located in
    $(LINK2 dmqproto/client/mixins/NeoSupport/NeoSupport.html, dmqproto.client.mixins.NeoSupport).
    The methods provided, along with usage examples, are here:
      * $(LINK2 dmqproto/client/mixins/NeoSupport/NeoSupport.Neo.html, Standard requests)
      * $(LINK2 dmqproto/client/mixins/NeoSupport/NeoSupport.TaskBlocking.html, Task-blocking requests)


    Documentation:

    For detailed documentation see dmqproto.client.legacy.README.


    Basic usage example:

    The following steps should be followed to set up and use the DMQ client:

        1. Create an EpollSelectDispatcher instance.
        2. Create a DmqClient instance, pass the epoll select dispatcher and
           the maximum number of connections per node as constructor argument.
        3. Add the DMQ nodes connection data by calling addNode() for each
           DMQ node to connect to. (Or simply call addNodes(), passing the
           path of an ini file describing the list of nodes to connect to.)
        4. Add one or multiple requests by calling one of the client request
           methods and assigning the resulting object.

    Example: Use at most five connections to each DMQ node, connect to nodes
    running at 192.168.1.234:56789 and 192.168.9.87:65432 and perform a Pop
    request.

    ---

        // Record value destination string
        mstring val;

        // Error flag, set to true when a request error occurs.
        bool error;

        // Request notification callback. Sets the error flag on failure.
        void notify ( DmqClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                error = true;
            }
        }

        // Callback delegate to receive value
        void receive_value ( DmqClient.RequestContext context, cstring value )
        {
            if ( value.length )
            {
                val.length = value.length;
                val[] = value[];
                // the above array copy can also be achieved using ocean.core.Array : copy
            }
        }


        // Initialise epoll -- Step 1
        auto epoll = new EpollSelectDispatcher;

        // Initialise DMQ client -- Step 2
        const NumConnections = 5;
        scope dmq = new DmqClient(epoll, NumConnections);

        // Add nodes -- Step 3
        dmq.addNode("192.168.1.234", 56789);
        dmq.addNode("192.168.9.87",  65432);

        // Perform a Pop request -- Step 4
        dmq.assign(dmq.pop("my_channel", &receive_value, &notify));
        epoll.eventLoop();

        // val now contains a value popped from the DMQ (or "" if the DMQ
        // was empty)

    ---


    Useful build flags:

    -debug=SwarmClient: trace outputs noting when requests begin, end, etc

    -debug=ISelectClient: trace outputs noting epoll registrations and events
        firing

    -debug=Raw: trace outputs noting raw data sent & received via epoll


    Copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved.

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dmqproto.client.DmqClient;


/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.util.ExtensibleClass;
import swarm.Const;

import swarm.client.model.IClient;
import swarm.client.model.ClientSettings;

import swarm.client.ClientExceptions;
import swarm.client.ClientCommandParams;

import swarm.client.connection.RequestOverflow;

import swarm.client.helper.GroupRequest;

import swarm.client.plugins.RequestQueueDiskOverflow;
import swarm.client.plugins.RequestScheduler;
import swarm.client.plugins.ScopeRequests;

import swarm.client.request.notifier.IRequestNotification;

import dmqproto.client.legacy.internal.RequestSetup;

import dmqproto.client.legacy.internal.registry.DmqNodeRegistry;

import swarm.client.request.notifier.IRequestNotification;

import dmqproto.client.legacy.internal.request.params.RequestParams;

import swarm.client.request.model.ISuspendableRequest;

import dmqproto.client.legacy.internal.request.model.IProducer;

import dmqproto.client.legacy.DmqConst;

import ocean.core.Enforce;

import ocean.transition;


/*******************************************************************************

    Extensible DMQ Client.

    Supported plugin classes can be passed as template parameters, an instance
    of each of these classes must be passed to the constructor. For each plugin
    class members may be added, depending on the particular plugin class.

    Note that the call to setPlugins(), in the class' ctors, *must* occur before
    the super ctor is called. This is because plugins may rely on the ctor being
    able to access their properly initialised instance, usually via an
    overridden method. The RequestQueueDiskOverflow plugin works like this, for
    example.

    Currently supported plugin classes:

        see dmqproto.client.legacy.internal.plugins package
        and swarm.client.plugins

*******************************************************************************/

public class ExtensibleDmqClient ( Plugins ... ) : DmqClient
{
    mixin ExtensibleClass!(Plugins);

    /***********************************************************************

        Constructor with support for only the legacy protocol. Automatically
        calls addNodes() with the node definition file specified in the Config
        instance.

        Params:
            epoll = EpollSelectDispatcher instance to use
            plugin_instances = instances of Plugins
            config = configuration class to use
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***********************************************************************/

    public this ( EpollSelectDispatcher epoll, Plugins plugin_instances,
        IClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, config, fiber_stack_size);
    }


    /***************************************************************************

        Constructor with support for only the legacy protocol. This constructor
        that accepts all arguments manually (i.e. not read from a config file)
        is mostly of use in tests.

        Params:
            epoll = EpollSelectDispatcher instance to use
            plugin_instances = instances of Plugins
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Plugins plugin_instances,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, conn_limit, queue_size, fiber_stack_size);
    }


     /***************************************************************************

        Constructor with support for the neo and legacy protocols. Automatically
        calls addNodes() with the node definition files specified in the legacy
        and neo Config instances.

        Params:
            epoll = EpollSelectDispatcher instance to use
            plugin_instances = instances of Plugins
            config = swarm.client.model.IClient.Config instance. (The Config
                class is designed to be read from an application's config.ini
                file via ocean.util.config.ConfigFiller.)
            neo_config = swarm.neo.client.mixins.ClientCore.Config instance.
                (The Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller.)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Plugins plugin_instances,
        IClient.Config config, Neo.Config neo_config,
        Neo.ConnectionNotifier conn_notifier,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, config, neo_config, conn_notifier, fiber_stack_size);
    }


    /***************************************************************************

        Constructor with support for the neo and legacy protocols. This
        constructor that accepts all arguments manually (i.e. not read from
        config files) is mostly of use in tests.

        Params:
            epoll = EpollSelectDispatcher instance to use
            auth_name = client name for authorisation
            auth_key = client key (password) for authorisation
            plugin_instances = instances of Plugins
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, cstring auth_name, ubyte[] auth_key,
        Plugins plugin_instances, Neo.ConnectionNotifier conn_notifier,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this.setPlugins(plugin_instances);

        super(epoll, auth_name, auth_key, conn_notifier, conn_limit, queue_size,
            fiber_stack_size);
    }
}


/*******************************************************************************

    Alias for a DmqClient with a scheduler

*******************************************************************************/

public class SchedulingDmqClient : ExtensibleDmqClient!(RequestScheduler)
{
    static class Config : IClient.Config
    {
        /***********************************************************************

            Limit on the number of events which can be managed by the scheduler
            at one time (0 = no limit)

        ***********************************************************************/

        uint scheduler_limit = 0;
    }


    /***********************************************************************

        Constructor with support for only the legacy protocol. Automatically
        calls addNodes() with the node definition file specified in the Config
        instance.

        Params:
            epoll = EpollSelectDispatcher instance to use
            config = configuration class to use
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***********************************************************************/

    public this ( EpollSelectDispatcher epoll,
        SchedulingDmqClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        super(epoll, new RequestScheduler(epoll, config.scheduler_limit),
            config, fiber_stack_size);
    }


    /***************************************************************************

        Constructor with support for only the legacy protocol. This constructor
        that accepts all arguments manually (i.e. not read from a config file)
        is mostly of use in tests.

        Params:
            epoll = EpollSelectDispatcher instance to use
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)
            max_events = limit on the number of events which can be managed
                by the scheduler at one time. (0 = no limit)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size,
        uint max_events = 0 )
    {
        super(epoll, new RequestScheduler(epoll, max_events), conn_limit,
            queue_size, fiber_stack_size);
    }


    /***************************************************************************

        Constructor with support for the neo and legacy protocols. This
        constructor that accepts all arguments manually (i.e. not read from
        config files) is mostly of use in tests.

        Params:
            epoll = EpollSelectorDispatcher instance to use
            auth_name = client name for authorisation
            auth_key = client key (password) for authorisation
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers
            max_events = limit on the number of events which can be managed
                by the scheduler at one time. (0 = no limit)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, char[] auth_name, ubyte[] auth_key,
        Neo.ConnectionNotifier conn_notifier,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size,
        uint max_events = 0 )
    {
        super(epoll, auth_name, auth_key,
            new RequestScheduler(epoll, max_events),
            conn_notifier, conn_limit, queue_size, fiber_stack_size);
    }


    /***************************************************************************

        Constructor with support for the neo and legacy protocols. Automatically
        calls addNodes() with the node definition files specified in the legacy
        and neo Config instances.

        Params:
            epoll = EpollSelectDispatcher instance to use
            config = SchedulingDmqClient.Config instance. (The Config class is
                designed to be read from an application's config.ini file via
                ocean.util.config.ConfigFiller.)
            neo_config = swarm.neo.client.mixins.ClientCore.Config instance.
                (The Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller.)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers
            max_events = limit on the number of events which can be managed
                by the scheduler at one time. (0 = no limit)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, SchedulingDmqClient.Config config,
        Neo.Config neo_config, Neo.ConnectionNotifier conn_notifier,
        size_t fiber_stack_size = IClient.default_fiber_stack_size,
        uint max_events = 0 )
    {
        super(epoll, new RequestScheduler(epoll, max_events),
            config, neo_config, conn_notifier, fiber_stack_size);
    }
}


/*******************************************************************************

    DmqClient

*******************************************************************************/

public class DmqClient : IClient
{
    /***************************************************************************

        Local alias definitions

    ***************************************************************************/

    public alias .RequestParams RequestParams;
    public alias .IRequestNotification RequestNotification;
    public alias .ISuspendableRequest ISuspendableRequest;
    public alias .IProducer IProducer;


    /***************************************************************************

        Plugin alias definitions

    ***************************************************************************/

    public alias .RequestScheduler RequestScheduler;

    public alias .RequestQueueDiskOverflow RequestQueueDiskOverflow;

    public alias .ScopeRequestsPlugin ScopeRequestsPlugin;


    /***************************************************************************

        Exceptions thrown in error cases.

    ***************************************************************************/

    private BadChannelNameException bad_channel_exception;


    /***************************************************************************

        Neo protocol support.

    ***************************************************************************/

    import dmqproto.client.mixins.NeoSupport;

    mixin NeoSupport!();


    /***************************************************************************

        Constructor with support for only the legacy protocol. Automatically
        calls addNodes() with the node definition file specified in the Config
        instance.

        Params:
            epoll = EpollSelectDispatcher instance to use
            config = Config instance (see swarm.client.model.IClient. The
                Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller)
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, IClient.Config config,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        with ( config )
        {
            this(epoll, connection_limit(), queue_size(), fiber_stack_size);

            this.addNodes(nodes_file);
        }
    }


     /***************************************************************************

        Constructor with support for the neo and legacy protocols. Accepts only
        `Neo.Config`, not `IClient.Config`, for applications that use the neo
        protocol only.

        Params:
            epoll = EpollSelectDispatcher instance to use
            neo_config = swarm.neo.client.mixins.ClientCore.Config instance.
                (The Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller.)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, Neo.Config neo_config,
        Neo.ConnectionNotifier conn_notifier,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this(epoll, conn_limit, queue_size, fiber_stack_size);

        this.neoInit(neo_config, conn_notifier);
    }


    /***************************************************************************

        Constructor with support for the neo and legacy protocols. This
        constructor that accepts all arguments manually (i.e. not read from
        config files) is mostly of use in tests.

        Params:
            epoll = EpollSelectDispatcher instance to use
            auth_name = client name for authorisation
            auth_key = client key (password) for authorisation. This should be a
                properly generated random number which only the client and the
                nodes know. See `README_client_neo.rst` for suggestions. The key
                must be of the length defined in
                swarm.neo.authentication.HmacDef (128 bytes)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            conn_limit = maximum number of connections to each DMQ node
            queue_size = maximum size of the per-node request queue
            fiber_stack_size = size of connection fibers' stack (in bytes)

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, cstring auth_name, ubyte[] auth_key,
        Neo.ConnectionNotifier conn_notifier,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        this(epoll, conn_limit, queue_size, fiber_stack_size);

        this.neoInit(auth_name, auth_key, conn_notifier);
    }


     /***************************************************************************

        Constructor with support for the neo and legacy protocols. Automatically
        calls addNodes() with the node definition files specified in the legacy
        and neo Config instances.

        Params:
            epoll = EpollSelectDispatcher instance to use
            config = swarm.client.model.IClient.Config instance. (The Config
                class is designed to be read from an application's config.ini
                file via ocean.util.config.ConfigFiller.)
            neo_config = swarm.neo.client.mixins.ClientCore.Config instance.
                (The Config class is designed to be read from an application's
                config.ini file via ocean.util.config.ConfigFiller.)
            conn_notifier = delegate which is called when a connection attempt
                succeeds or fails (including when a connection is
                re-established). Of type:
                void delegate ( IPAddress node_address, Exception e )
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, IClient.Config config,
        Neo.Config neo_config, Neo.ConnectionNotifier conn_notifier,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        with ( config )
        {
            this(epoll, connection_limit(), queue_size(), fiber_stack_size);

            this.addNodes(nodes_file);
        }

        this.neoInit(neo_config, conn_notifier);
    }


    /***************************************************************************

        Constructor

        Params:
            epoll = select dispatcher to use
            conn_limit  = maximum number of connections in pool
            queue_size = size (in bytes) of per-node queue of pending requests
            fiber_stack_size = size (in bytes) of stack of individual connection
                fibers

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll,
        size_t conn_limit = IClient.Config.default_connection_limit,
        size_t queue_size = IClient.Config.default_queue_size,
        size_t fiber_stack_size = IClient.default_fiber_stack_size )
    {
        ClientSettings settings;
        settings.conn_limit = conn_limit;
        settings.queue_size = queue_size;
        settings.fiber_stack_size = fiber_stack_size;

        auto node_registry = new DmqNodeRegistry(epoll, settings,
            this.requestOverflow, this.errorReporter);
        super(epoll, node_registry);

        this.bad_channel_exception = new BadChannelNameException;
    }


    /***************************************************************************

        Adds a node connection to the registry.

        Params:
            address = node address
            port = node service port

        Throws:
            exception if the node already exists in the registry

    ***************************************************************************/

    override public void addNode ( char[] host, ushort port )
    {
        this.registry.add(host, port);
    }


    /***************************************************************************

        Assigns a new request to the client. The request is validated, and the
        notification callback may be invoked immediately if any errors are
        detected. Otherwise the request is sent to the node registry, where it
        will be either executed immediately (if a free connection is available)
        or queued for later execution.

        Template params:
            T = request type (should be one of the structs defined in this
                module)

        Params:
            request = request to assign

    ***************************************************************************/

    public void assign ( T ) ( T request )
    {
        static if ( is(T : IGroupRequest) )
        {
            request.setClient(this);
        }

        this.scopeRequestParams(
            ( IRequestParams params )
            {
                request.setup(params);
                this.assignParams(params);
            });
    }


    /***************************************************************************

        Creates a Push request instance ready to be assigned. The record value
        is taken from the input delegate, which should be of the type:

            cstring delegate ( DmqClient.RequestContext )

        If a Push request is attempted on a node and fails, it will be
        automatically re-tried on any other nodes in the DMQ system, until one
        succeeds or all have been tried and failed. At this point, the notifier
        delegate will be called with the GroupFinished notification. (Note that,
        in the current implementation, the GroupFinished notification will occur
        *before* the final Finished notification.)

        Params:
            channel = DMQ channel to push to
            input = input delegate to take record value from
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Push
    {
        mixin RequestBase;
        mixin IODelegate!("put_value"); // io(PutValueDg) method
        mixin Channel;                  // channel(cstring) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Push push ( cstring channel, RequestParams.PutValueDg input,
            RequestNotification.Callback notifier )
    {
        return *Push(DmqConst.Command.E.Push, notifier)
            .channel(channel).io(input);
    }


    /***************************************************************************

        Creates a PushMulti request instance ready to be assigned, which pushes
        a single value into multiple DMQ channels. The record value is taken
        from the input delegate, which should be of the type:

            cstring delegate ( DmqClient.RequestContext context )

        If a PushMulti request is attempted on a node and fails with any error
        except OutOfMemory*, it will be automatically re-tried on any other
        nodes in the DMQ system, until one succeeds or all have been tried and
        failed. At this point, the notifier delegate will be called with the
        GroupFinished notification. (Note that, in the current implementation,
        the GroupFinished notification will occur *before* the final Finished
        notification.)

        * The reason why the request will not be retried in the case where it
        fails due to an OutOfMemory error (i.e. "DMQ full") is that the DMQ
        node doesn't give any feedback as to which of the specified channels
        failed to receive the record. For example, when pushing to channels
        [A, B, C], the record may be successfully written to A and B, with only
        C being too full to receive the record. In this case, simply retrying
        the request to the next node could result in the record being written
        again to A and B in another node -- a duplication of these records.

        Params:
            channels = list of DMQ channels to push to
            input = input delegate to take record value from
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct PushMulti
    {
        mixin RequestBase;
        mixin IODelegate!("put_value"); // io(PutValueDg) method
        mixin Channels;                 // channels(in cstring[]) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public PushMulti pushMulti ( in cstring[] channels, RequestParams.PutValueDg input,
            RequestNotification.Callback notifier )
    {
        return *PushMulti(DmqConst.Command.E.PushMulti, notifier)
            .channels(channels).io(input);
    }


    /***************************************************************************

        Creates a Pop request instance ready to be assigned. The record value
        received from the DMQ node is sent to the output delegate (an empty
        string meaning that the DMQ is empty).

        The output delegate is of the following form:

            void delegate ( DmqClient.RequestContext context, cstring value )

        If a Pop request is attempted on a node and receives an empty record, it
        will be automatically re-tried on any other nodes in the DMQ system,
        until one receives a record or all have been tried and returned nothing.
        At this point, the notifier delegate will be called with the
        GroupFinished notification. (Note that, in the current implementation,
        the GroupFinished notification will occur *before* the final Finished
        notification.)

        Params:
            channel = DMQ channel to pop from
            output = output delegate to send record value to
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Pop
    {
        mixin RequestBase;
        mixin IODelegate!("get_value"); // io(GetValueDg) method
        mixin Channel;                  // channel(cstring) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Pop pop ( cstring channel, RequestParams.GetValueDg output,
            RequestNotification.Callback notifier )
    {
        return *Pop(DmqConst.Command.E.Pop, notifier)
            .channel(channel).io(output);
    }


    /***************************************************************************

        Creates a Consume request instance ready to be assigned. The DMQ
        record values are sent to output as they are received.

        The output delegate is of the following form:

            void delegate ( DmqClient.RequestContext context, cstring value )

        Params:
            channel = DMQ channel to read from
            output  = output delegate to send record values to
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Consume
    {
        mixin RequestBase;
        mixin IODelegate!("get_value"); // io(GetValueDg) method
        mixin Node;                     // node(NodeItem) method
        mixin Channel;                  // channel(cstring) method
        mixin Suspendable;              // suspendable(RequestParams.RegisterSuspendableDg) method
        mixin StreamInfo;               // stream_info(RequestParams.RegisterStreamInfoDg) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Consume consume ( cstring channel, RequestParams.GetValueDg output,
            RequestNotification.Callback notifier )
    {
        return *Consume(DmqConst.Command.E.Consume, notifier)
            .channel(channel).io(output);
    }


    /***************************************************************************

        Creates a Produce request instance ready to be assigned. The producer
        delegate is called when the Produce request is begun, and provides the
        user with an IProducer interface with a single method (opCall / produce)
        which accepts a cstring to be pushed to the DMQ. After the produce()
        method has been called, the provided value will be sent to the node.
        When the value has been sent, the producer delegate will be called again
        with another IProduce interface which may then be used to provide
        another value to send. This process repeats until the produce() method
        of the delegate is called with an empty string, which will terminate the
        produce request.

        Beware that only a *single value* should be sent to the IProducer
        interface each time it is provided. Further values sent to the same
        interface may be ignored.

        Note that a value does not have to be provided to the IProducer
        interface immediately -- it is perfectly fine to keep a copy of the
        interface to be used when a value is ready to be sent.

        The producer delegate is of the following form:

            void delegate ( DmqClient.RequestContext context, IProducer producer )

        Params:
            channel = DMQ channel to write to
            producer = producer delegate to send IProducer interface to
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Produce
    {
        mixin RequestBase;
        mixin IODelegate!("producer"); // io(ProducerDg) method
        mixin Node;                    // node(NodeItem) method
        mixin Channel;                 // channel(cstring) method
        mixin StreamInfo;              // stream_info(RequestParams.RegisterStreamInfoDg) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Produce produce ( cstring channel, RequestParams.ProducerDg producer,
            RequestNotification.Callback notifier )
    {
        return *Produce(DmqConst.Command.E.Produce, notifier)
            .channel(channel).io(producer);
    }


    /***************************************************************************

        Creates a ProduceMulti request instance ready to be assigned. The
        producer delegate is called when the ProduceMulti request is begun, and
        provides the user with an IProducer interface with a single method
        (opCall / produce) which accepts a cstring to be pushed to the specified
        channels in the DMQ. After the produce() method has been called, the
        provided value will be sent to the node. When the value has been sent,
        the producer delegate will be called again with another IProduce
        interface which may then be used to provide another value to send. This
        process repeats until the produce() method of the delegate is called
        with an empty string, which will terminate the produce multi request.

        Beware that only a *single value* should be sent to the IProducer
        interface each time it is provided. Further values sent to the same
        interface may be ignored.

        Note that a value does not have to be provided to the IProducer
        interface immediately -- it is perfectly fine to keep a copy of the
        interface to be used when a value is ready to be sent.

        The producer delegate is of the following form:

            void delegate ( DmqClient.RequestContext context, IProducer producer )

        Params:
            channels = DMQ channels to write to
            producer = producer delegate to send IProducer interface to
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct ProduceMulti
    {
        mixin RequestBase;
        mixin IODelegate!("producer"); // io(ProducerDg) method
        mixin Node;                    // node(NodeItem) method
        mixin Channels;                // channels(in cstring[]) method
        mixin StreamInfo;              // stream_info(RequestParams.RegisterStreamInfoDg) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public ProduceMulti produceMulti ( in cstring[] channels,
        RequestParams.ProducerDg producer, RequestNotification.Callback notifier )
    {
        return *ProduceMulti(DmqConst.Command.E.ProduceMulti, notifier).
            channels(channels).io(producer);
    }


    /***************************************************************************

        Creates a GetChannels request instance ready to be assigned. The DMQ
        channel names are sent to the output delegate.

        The output delegate is of the following form:

            void delegate ( RequestContext context, cstring address, ushort port,
                    cstring channel )

        This is a bulk command which is executed over all nodes asynchronously.
        This means that the received data will not be in any defined order,
        being received from all nodes in parallel, and that, in a system with
        multiple DMQ nodes, the name of each channel will be received once
        per node on which it exists.

        The output delegate is called once per node.

        Params:
            output = getter delegate to receive channel names
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetChannels
    {
        mixin RequestBase;
        mixin IODelegate!("get_node_value"); // io(GetNodeValueDg) method
        mixin Node;                          // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetChannels getChannels ( RequestParams.GetNodeValueDg output,
        RequestNotification.Callback notifier )
    {
        return *GetChannels(DmqConst.Command.E.GetChannels, notifier).io(output);
    }


    /***************************************************************************

        Creates a GetSize request instance ready to be assigned.

        The size info is received by a delegate of the following form:

            void delegate ( DmqClient.RequestContext context, cstring node_address, ushort node_port, ulong records, ulong bytes )

        This is a bulk command which is executed over all nodes asynchronously.
        This means that the received data will not be in any defined order,
        being received from all nodes in parallel.

        The output delegate is called once per node.

        Params:
            output = delegate to receive node size info
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetSize
    {
        mixin RequestBase;
        mixin IODelegate!("get_size_info"); // io(GetSizeInfoDg) method
        mixin Node;                         // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetSize getSize ( RequestParams.GetSizeInfoDg output,
        RequestNotification.Callback notifier )
    {
        return *GetSize(DmqConst.Command.E.GetSize, notifier).io(output);
    }


    /***************************************************************************

        Creates a GetChannelSize request instance ready to be assigned.

        The size info is received by a delegate of the following form:

            void delegate ( DmqClient.RequestContext context, cstring node_address, ushort node_port, cstring channel, ulong records, ulong bytes )

        This is a bulk command which is executed over all nodes asynchronously.
        This means that the received data will not be in any defined order,
        being received from all nodes in parallel.

        The output delegate is called once per node.

        Params:
            output = delegate to receive channel size info
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetChannelSize
    {
        mixin RequestBase;
        mixin IODelegate!("get_channel_size"); // io(GetChannelSizeInfoDg) method
        mixin Channel;                         // channel(cstring) method
        mixin Node;                            // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetChannelSize getChannelSize ( cstring channel,
        RequestParams.GetChannelSizeInfoDg output, RequestNotification.Callback notifier )
    {
        return *GetChannelSize(DmqConst.Command.E.GetChannelSize, notifier)
            .channel(channel).io(output);
    }


    /***************************************************************************

        Creates a RemoveChannel request, which will delete all records from the
        specified channel in all nodes of the DMQ.

        This is a multi-node request which is executed in parallel over all
        nodes in the DMQ.

        Params:
            channel = DMQ channel
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct RemoveChannel
    {
        mixin RequestBase;
        mixin Channel;          // channel(cstring) method
        mixin Node;             // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public RemoveChannel removeChannel ( cstring channel, RequestNotification.Callback notifier )
    {
        return *RemoveChannel(DmqConst.Command.E.RemoveChannel, notifier)
            .channel(channel);
    }


    /***************************************************************************

        Creates a GetnumConnections request instance ready to be assigned.

        The number of connections is received by a delegate of the following
        form:

            void delegate ( DmqClient.RequestContext context, cstring node_address, ushort node_port, size_t num_connections )

        This is a bulk command which is executed over all nodes asynchronously.
        This means that the received data will not be in any defined order,
        being received from all nodes in parallel.

        The output delegate is called once per node.

        Params:
            output = delegate to forward connections info to
            notifier = callback for notifications about the request

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct GetNumConnections
    {
        mixin RequestBase;
        mixin IODelegate!("get_num_connections"); // io(GetNumConnectionsDg) method
        mixin Node;                               // node(NodeItem) method

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public GetNumConnections getNumConnections ( RequestParams.GetNumConnectionsDg output, RequestNotification.Callback notifier )
    {
        return *GetNumConnections(DmqConst.Command.E.GetNumConnections, notifier)
            .io(output);
    }


    /***************************************************************************

        Creates a Flush client-command, which causes the client to flush the
        write buffers of all streaming write requests (e.g. Produce,
        ProduceMulti).

        Params:
            notifier = notification callback

        Returns:
            instance allowing optional settings to be set and then to be passed
            to assign()

    ***************************************************************************/

    private struct Flush
    {
        mixin ClientCommandBase;

        mixin RequestParamsSetup; // private setup() method, used by assign()
    }

    public Flush flush ( IRequestNotification.Callback notifier )
    {
        return Flush(ClientCommandParams.Command.Flush, notifier);
    }


    /***************************************************************************

        Creates a new request params instance (derived from IRequestParams), and
        passes it to the provided delegate.

        This method is used by the request scheduler plugin, which needs to be
        able to construct and use a request params instance without knowing
        which derived type is used by the client.

        Params:
            dg = delegate to receive and use created scope IRequestParams
                instance

    ***************************************************************************/

    override protected void scopeRequestParams (
        void delegate ( IRequestParams params ) dg )
    {
        scope params = new RequestParams;
        dg(params);
    }


    /***************************************************************************

        Checks whether the given request params are valid.

        Params:
            params = request params to check

        Throws:
            * if the channel name is invalid

            (exceptions will be caught in super.assignParams)

    ***************************************************************************/

    override protected void validateRequestParams_ ( IRequestParams params )
    {
        auto dmq_params = cast(RequestParams)params;

        // Validate channel name(s), for commands which use them
        with ( DmqConst.Command.E ) switch ( params.command )
        {
            case Push:
            case Pop:
            case GetChannelSize:
            case Consume:
            case Produce:
                enforce(this.bad_channel_exception,
                    .validateChannelName(dmq_params.channel));
            break;

            case PushMulti:
                foreach ( channel; dmq_params.channels )
                {
                    enforce(this.bad_channel_exception,
                        .validateChannelName(channel));
                }
            break;

            default:
        }
    }
}

version ( UnitTest )
{
    import ocean.io.select.EpollSelectDispatcher;
    import swarm.client.request.params.IRequestParams;
}

/*******************************************************************************

    Test instantiating clients with various plugins.

*******************************************************************************/

unittest
{
    auto epoll = new EpollSelectDispatcher;

    {
        auto dmq = new ExtensibleDmqClient!(DmqClient.RequestScheduler)
            (epoll, new RequestScheduler(epoll));
    }

    {
        class DummyStore : RequestQueueDiskOverflow.IRequestStore
        {
            ubyte[] store ( IRequestParams params ) { return null; }
            void restore ( void[] stored ) { }
        }

        auto dmq = new ExtensibleDmqClient!(DmqClient.RequestQueueDiskOverflow)
            (epoll, new RequestQueueDiskOverflow(new DummyStore, "dummy"));
    }

    {
        auto dmq = new ExtensibleDmqClient!(DmqClient.ScopeRequestsPlugin)
            (epoll, new ScopeRequestsPlugin);
    }
}
